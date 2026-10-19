package com.github.aznamier.keycloak.event.provider;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.jboss.logging.Logger;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;

/**
 * Sends essential events and reports whether RabbitMQ confirmed them, so the Keycloak request can be rolled back
 * when they were not.
 *
 * Messages are sent one at a time from a dedicated thread on a dedicated channel in confirm mode, as mandatory
 * messages, so a message no queue would receive counts as a failure. The calling request thread waits for the
 * result up to the essential events timeout, which therefore also covers a publish blocked by RabbitMQ.
 *
 * Internal to the plugin, so it can change in any release.
 */
class RabbitMqEssentialPublisher {

	private static final Logger log = Logger.getLogger(RabbitMqEssentialPublisher.class);

	private static final String NOT_CONFIRMED_IN_TIME = "not confirmed within the essential events timeout";

	private final RabbitMqConnectionManager connectionManager;
	private final RabbitMqConfig cfg;
	private final LinkedBlockingQueue<EssentialMessage> queue = new LinkedBlockingQueue<>();
	private final Thread worker;
	private volatile boolean closed;

	RabbitMqEssentialPublisher(RabbitMqConnectionManager connectionManager, RabbitMqConfig cfg) {
		this.connectionManager = connectionManager;
		this.cfg = cfg;
		worker = new Thread(this::sendLoop, "keycloak-to-rabbitmq-essential");
		worker.setDaemon(true);
		worker.start();
	}

	/**
	 * Queues the message for sending. The returned future completes once RabbitMQ has confirmed it, or fails
	 * straight away if RabbitMQ is not connected.
	 */
	CompletableFuture<Void> send(String routingKey, BasicProperties props, byte[] body, long deadline) {
		EssentialMessage msg = new EssentialMessage(routingKey, props, body, deadline);
		if (closed) {
			msg.result.completeExceptionally(new IOException("shutting down"));
		} else if (connectionManager.getEssentialChannel() == null) {
			msg.result.completeExceptionally(new IOException("not connected to RabbitMQ"));
		} else {
			queue.add(msg);
		}
		return msg.result;
	}

	/**
	 * Waits until the message is confirmed or the deadline passes. Throws with the reason if it was not confirmed;
	 * a message that timed out is not sent afterwards (unless it was already being sent).
	 */
	static void await(CompletableFuture<Void> result, long deadline) throws Exception {
		try {
			result.get(Math.max(1, deadline - System.currentTimeMillis()), TimeUnit.MILLISECONDS);
		} catch (TimeoutException e) {
			result.cancel(false);
			throw new TimeoutException(NOT_CONFIRMED_IN_TIME);
		} catch (ExecutionException e) {
			throw e.getCause() instanceof Exception ? (Exception) e.getCause() : e;
		}
	}

	void close() {
		closed = true;
		worker.interrupt();
		EssentialMessage msg;
		while ((msg = queue.poll()) != null) {
			msg.result.completeExceptionally(new IOException("shutting down"));
		}
	}

	private void sendLoop() {
		while (!closed) {
			EssentialMessage msg;
			try {
				msg = queue.take();
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				return;
			}
			// The request already gave up waiting and is rolled back, so the event must not be sent
			if (msg.result.isDone() || System.currentTimeMillis() >= msg.deadline) {
				msg.result.completeExceptionally(new TimeoutException(NOT_CONFIRMED_IN_TIME));
				continue;
			}
			try {
				sendAndConfirm(msg);
				msg.result.complete(null);
				log.tracef("keycloak-to-rabbitmq SUCCESS sending essential message: %s%n", msg.routingKey);
			} catch (InterruptedException e) {
				msg.result.completeExceptionally(new IOException("shutting down"));
				Thread.currentThread().interrupt();
				return;
			} catch (Exception e) {
				msg.result.completeExceptionally(e);
			}
		}
	}

	private void sendAndConfirm(EssentialMessage msg) throws Exception {
		Channel channel = connectionManager.getEssentialChannel();
		if (channel == null) {
			throw new IOException("not connected to RabbitMQ");
		}
		String messageId = msg.props.getMessageId();
		long expectedSeqNo = channel.getNextPublishSeqNo() + 1;
		channel.basicPublish(cfg.getExchange(), msg.routingKey, true, msg.props, msg.body);
		boolean acked;
		try {
			acked = channel.waitForConfirms(Math.max(1, msg.deadline - System.currentTimeMillis()));
		} catch (TimeoutException e) {
			// The client's TimeoutException has no message; this one can reach the request first, so it gives the reason
			throw new TimeoutException(NOT_CONFIRMED_IN_TIME);
		}
		// A channel recovered after a connection drop starts with nothing to confirm, so waitForConfirms
		// on it succeeds even though the message was published on the old connection
		if (channel.getNextPublishSeqNo() != expectedSeqNo) {
			throw new IOException("connection to RabbitMQ was recovered before the event was confirmed");
		}
		// RabbitMQ sends the return of an unroutable mandatory message before its confirm
		if (connectionManager.consumeReturned(messageId)) {
			throw new IOException("no queue is bound to receive it (returned as unroutable)");
		}
		if (!acked) {
			throw new IOException("RabbitMQ rejected (nacked) it");
		}
	}

	private static final class EssentialMessage {
		private final String routingKey;
		private final BasicProperties props;
		private final byte[] body;
		private final long deadline;
		private final CompletableFuture<Void> result = new CompletableFuture<>();

		EssentialMessage(String routingKey, BasicProperties props, byte[] body, long deadline) {
			this.routingKey = routingKey;
			this.props = props;
			this.body = body;
			this.deadline = deadline;
		}
	}
}
