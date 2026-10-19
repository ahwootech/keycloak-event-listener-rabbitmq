package com.github.aznamier.keycloak.event.provider;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.jboss.logging.Logger;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;

/**
 * Sends messages to RabbitMQ, either directly on the calling thread (publish_mode sync, the default) or, with
 * publish_mode async, from a single background thread so Keycloak requests never wait on the broker.
 *
 * In async mode, messages wait in an in-memory buffer while RabbitMQ is unreachable and are sent in order
 * once the connection is back. When the buffer reaches its size limit, new messages are dropped and logged.
 *
 * With publish_mode confirmed, messages are sent from the background thread in batches and only removed from the buffer
 * once RabbitMQ confirms them; a batch that is not confirmed is sent again, so consumers may see duplicates.
 *
 * Internal to the plugin, so it can change in any release.
 */
class RabbitMqPublisher {

	private static final Logger log = Logger.getLogger(RabbitMqPublisher.class);

	// Prefix for every message that is never sent, so it can be alerted on
	static final String PUBLISH_FAILED = "keycloak-to-rabbitmq PUBLISH FAILED";

	// Approximate memory used per buffered message on top of its body (routing key, properties, headers, queue node)
	private static final int MESSAGE_OVERHEAD_BYTES = 512;
	private static final long POLL_INTERVAL_MS = 500;
	private static final long RETRY_INTERVAL_MS = 1000;
	private static final long DROP_REPORT_INTERVAL_MS = 60_000;
	private static final int CONFIRM_BATCH_SIZE = 100;

	private final RabbitMqConnectionManager connectionManager;
	private final RabbitMqConfig cfg;

	// Async mode only
	private final long maxBufferBytes;
	private final LinkedBlockingQueue<PendingMessage> buffer = new LinkedBlockingQueue<>();
	private final AtomicLong bufferedBytes = new AtomicLong();
	private final AtomicLong droppedSinceReport = new AtomicLong();
	private final AtomicLong lastDropReport = new AtomicLong();
	private final Thread worker;
	private final boolean confirms;
	private volatile boolean closing;
	private volatile long flushDeadline;
	private volatile boolean halfFullWarned;

	RabbitMqPublisher(RabbitMqConnectionManager connectionManager, RabbitMqConfig cfg) {
		this.connectionManager = connectionManager;
		this.cfg = cfg;
		this.maxBufferBytes = cfg.getPublishBufferMaxMb() * 1024L * 1024L;
		this.confirms = confirmsEnabled(cfg);
		if (cfg.getPublishMode() != RabbitMqConfig.PublishMode.SYNC) {
			worker = new Thread(this::runWorker, "keycloak-to-rabbitmq-publisher");
			worker.setDaemon(true);
			worker.start();
		} else {
			worker = null;
		}
	}

	static boolean confirmsEnabled(RabbitMqConfig cfg) {
		return cfg.getPublishMode() == RabbitMqConfig.PublishMode.CONFIRMED;
	}

	void publish(String routingKey, BasicProperties props, byte[] body) {
		if (worker == null) {
			publishNow(routingKey, props, body);
		} else {
			enqueue(new PendingMessage(routingKey, props, body));
		}
	}

	/**
	 * Async mode: waits up to the shutdown flush timeout for buffered messages to be sent.
	 */
	void close() {
		if (worker == null) {
			return;
		}
		flushDeadline = System.currentTimeMillis() + cfg.getShutdownFlushTimeoutMs();
		closing = true;
		try {
			// Allow for a send or confirm wait that started just before the deadline
			long confirmWaitMs = confirms ? cfg.getConfirmTimeoutMs() : 0;
			worker.join(cfg.getShutdownFlushTimeoutMs() + POLL_INTERVAL_MS + RETRY_INTERVAL_MS + confirmWaitMs);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		if (worker.isAlive()) {
			worker.interrupt();
			log.errorf("%s: shutting down, publisher did not finish within %d ms, %d buffered event(s) not sent",
					PUBLISH_FAILED, cfg.getShutdownFlushTimeoutMs(), buffer.size());
		}
	}

	private void publishNow(String routingKey, BasicProperties props, byte[] body) {
		Channel channel = connectionManager.getChannel();
		if (channel == null) {
			log.errorf("keycloak-to-rabbitmq ERROR sending message, not connected to RabbitMQ: %s%n", routingKey);
			return;
		}
		try {
			channel.basicPublish(cfg.getExchange(), routingKey, props, body);
			log.tracef("keycloak-to-rabbitmq SUCCESS sending message: %s%n", routingKey);
		} catch (Exception ex) {
			log.errorf(ex, "keycloak-to-rabbitmq ERROR sending message: %s%n", routingKey);
		}
	}

	private void enqueue(PendingMessage msg) {
		if (closing) {
			log.errorf("%s: shutting down, event not sent: %s", PUBLISH_FAILED, msg.routingKey);
			return;
		}
		long total = bufferedBytes.addAndGet(msg.sizeBytes);
		if (total > maxBufferBytes) {
			bufferedBytes.addAndGet(-msg.sizeBytes);
			droppedSinceReport.incrementAndGet();
			reportDrops(msg.routingKey, false);
			return;
		}
		buffer.add(msg);
		if (!halfFullWarned && total > maxBufferBytes / 2) {
			halfFullWarned = true;
			log.warnf("keycloak-to-rabbitmq WARNING publish buffer is over half full: %d event(s), %.1f of %d MB",
					buffer.size(), total / (1024.0 * 1024.0), cfg.getPublishBufferMaxMb());
		}
	}

	/**
	 * Logs dropped messages at most once per report interval (unless forced), so a long outage doesn't flood the logs.
	 */
	private void reportDrops(String latestRoutingKey, boolean force) {
		long now = System.currentTimeMillis();
		long last = lastDropReport.get();
		if ((!force && now - last < DROP_REPORT_INTERVAL_MS) || droppedSinceReport.get() == 0
				|| !lastDropReport.compareAndSet(last, now)) {
			return;
		}
		long dropped = droppedSinceReport.getAndSet(0);
		if (latestRoutingKey != null) {
			log.errorf("%s: publish buffer full (%d MB), %d event(s) dropped since last report, latest: %s",
					PUBLISH_FAILED, cfg.getPublishBufferMaxMb(), dropped, latestRoutingKey);
		} else {
			log.errorf("%s: publish buffer full (%d MB), %d event(s) dropped since last report",
					PUBLISH_FAILED, cfg.getPublishBufferMaxMb(), dropped);
		}
	}

	private void runWorker() {
		try {
			sendLoop();
		} finally {
			// Report drops not yet covered by a periodic report
			reportDrops(null, true);
		}
	}

	private void sendLoop() {
		List<PendingMessage> batch = new ArrayList<>();
		// With confirms: whether the current batch may have been (partly) published, but was not confirmed
		boolean maybeSent = false;
		// With confirms: after a batch fails, its events are resent one at a time, so events RabbitMQ accepts
		// are not held back by one it keeps rejecting (e.g. a full queue with overflow=reject-publish)
		boolean oneByOne = false;
		boolean stalled = false;
		while (true) {
			try {
				if (batch.isEmpty()) {
					PendingMessage first = buffer.poll(POLL_INTERVAL_MS, TimeUnit.MILLISECONDS);
					if (first != null) {
						batch.add(first);
						if (confirms) {
							fillBatch(batch);
						}
					}
				}
				reportDrops(null, false);
				if (batch.isEmpty()) {
					if (closing) {
						return;
					}
					continue;
				}
				if (closing && System.currentTimeMillis() > flushDeadline) {
					int notSent = buffer.size() + batch.size();
					if (maybeSent) {
						log.errorf("%s: shutting down, %d buffered event(s) not sent, %d of them may have been sent but were not confirmed",
								PUBLISH_FAILED, notSent, batch.size());
					} else {
						log.errorf("%s: shutting down, %d buffered event(s) not sent", PUBLISH_FAILED, notSent);
					}
					return;
				}

				List<PendingMessage> sending = oneByOne ? batch.subList(0, 1) : batch;
				Channel channel = connectionManager.getChannel();
				Exception error = null;
				if (channel != null) {
					try {
						maybeSent = confirms;
						send(channel, sending);
					} catch (Exception e) {
						error = e;
					}
				}

				if (channel != null && error == null) {
					if (log.isTraceEnabled()) {
						for (PendingMessage msg : sending) {
							log.tracef("keycloak-to-rabbitmq SUCCESS sending message: %s%n", msg.routingKey);
						}
					}
					if (stalled) {
						stalled = false;
						log.infof("keycloak-to-rabbitmq sending resumed, %d buffered event(s) left to send", buffer.size());
					}
					long sentBytes = 0;
					for (PendingMessage msg : sending) {
						sentBytes += msg.sizeBytes;
					}
					long remaining = bufferedBytes.addAndGet(-sentBytes);
					if (remaining == 0) {
						halfFullWarned = false;
					}
					sending.clear();
					if (batch.isEmpty()) {
						maybeSent = false;
						oneByOne = false;
					}
				} else {
					oneByOne = confirms;
					// Keep the batch and retry; it stays at the front so order is preserved
					if (!stalled) {
						stalled = true;
						if (error != null) {
							log.errorf(error, "keycloak-to-rabbitmq ERROR sending message, buffering events until it succeeds: %s",
									batch.get(0).routingKey);
						} else {
							log.errorf("keycloak-to-rabbitmq ERROR sending message, not connected to RabbitMQ, buffering events until reconnected: %s",
									batch.get(0).routingKey);
						}
					}
					Thread.sleep(RETRY_INTERVAL_MS);
				}
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				return;
			}
		}
	}

	/**
	 * Adds buffered messages to the batch, up to the batch size.
	 */
	private void fillBatch(List<PendingMessage> batch) {
		buffer.drainTo(batch, CONFIRM_BATCH_SIZE - batch.size());
	}

	/**
	 * Publishes the batch and, with publisher confirms, waits until RabbitMQ has confirmed all of it.
	 * Throws if any message could not be published or was not confirmed.
	 */
	private void send(Channel channel, List<PendingMessage> batch) throws Exception {
		long firstSeqNo = confirms ? channel.getNextPublishSeqNo() : 0;
		for (PendingMessage msg : batch) {
			channel.basicPublish(cfg.getExchange(), msg.routingKey, msg.props, msg.body);
		}
		if (!confirms) {
			return;
		}
		long expectedSeqNo = firstSeqNo + batch.size();
		checkNotRecovered(channel, expectedSeqNo);
		boolean acked = channel.waitForConfirms(cfg.getConfirmTimeoutMs());
		// A channel recovered after a connection drop starts with nothing to confirm, so waitForConfirms
		// on it succeeds even though the batch was published on the old connection
		checkNotRecovered(channel, expectedSeqNo);
		if (!acked) {
			throw new IOException("RabbitMQ rejected (nacked) at least one of " + batch.size() + " event(s)");
		}
	}

	private static void checkNotRecovered(Channel channel, long expectedSeqNo) throws IOException {
		if (channel.getNextPublishSeqNo() != expectedSeqNo) {
			throw new IOException("connection to RabbitMQ was recovered before the events were confirmed");
		}
	}

	private static final class PendingMessage {
		private final String routingKey;
		private final BasicProperties props;
		private final byte[] body;
		private final long sizeBytes;

		PendingMessage(String routingKey, BasicProperties props, byte[] body) {
			this.routingKey = routingKey;
			this.props = props;
			this.body = body;
			this.sizeBytes = body.length + 2L * routingKey.length() + MESSAGE_OVERHEAD_BYTES;
		}
	}
}
