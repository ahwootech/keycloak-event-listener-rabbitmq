package com.github.aznamier.keycloak.event.provider;

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

	private final RabbitMqConnectionManager connectionManager;
	private final RabbitMqConfig cfg;

	// Async mode only
	private final long maxBufferBytes;
	private final LinkedBlockingQueue<PendingMessage> buffer = new LinkedBlockingQueue<>();
	private final AtomicLong bufferedBytes = new AtomicLong();
	private final AtomicLong droppedSinceReport = new AtomicLong();
	private final AtomicLong lastDropReport = new AtomicLong();
	private final Thread worker;
	private volatile boolean closing;
	private volatile long flushDeadline;
	private volatile boolean halfFullWarned;

	RabbitMqPublisher(RabbitMqConnectionManager connectionManager, RabbitMqConfig cfg) {
		this.connectionManager = connectionManager;
		this.cfg = cfg;
		this.maxBufferBytes = cfg.getPublishBufferMaxMb() * 1024L * 1024L;
		if (cfg.getPublishMode() != RabbitMqConfig.PublishMode.SYNC) {
			worker = new Thread(this::runWorker, "keycloak-to-rabbitmq-publisher");
			worker.setDaemon(true);
			worker.start();
		} else {
			worker = null;
		}
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
			// Allow for a send that started just before the deadline
			worker.join(cfg.getShutdownFlushTimeoutMs() + POLL_INTERVAL_MS + RETRY_INTERVAL_MS);
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
		PendingMessage msg = null;
		boolean stalled = false;
		while (true) {
			try {
				if (msg == null) {
					msg = buffer.poll(POLL_INTERVAL_MS, TimeUnit.MILLISECONDS);
				}
				reportDrops(null, false);
				if (msg == null) {
					if (closing) {
						return;
					}
					continue;
				}
				if (closing && System.currentTimeMillis() > flushDeadline) {
					log.errorf("%s: shutting down, %d buffered event(s) not sent", PUBLISH_FAILED, buffer.size() + 1);
					return;
				}

				Channel channel = connectionManager.getChannel();
				Exception error = null;
				if (channel != null) {
					try {
						channel.basicPublish(cfg.getExchange(), msg.routingKey, msg.props, msg.body);
					} catch (Exception e) {
						error = e;
					}
				}

				if (channel != null && error == null) {
					log.tracef("keycloak-to-rabbitmq SUCCESS sending message: %s%n", msg.routingKey);
					if (stalled) {
						stalled = false;
						log.infof("keycloak-to-rabbitmq sending resumed, %d buffered event(s) left to send", buffer.size());
					}
					if (bufferedBytes.addAndGet(-msg.sizeBytes) == 0) {
						halfFullWarned = false;
					}
					msg = null;
				} else {
					// Keep the message and retry; it stays at the front so order is preserved
					if (!stalled) {
						stalled = true;
						if (error != null) {
							log.errorf(error, "keycloak-to-rabbitmq ERROR sending message, buffering events until it succeeds: %s",
									msg.routingKey);
						} else {
							log.errorf("keycloak-to-rabbitmq ERROR sending message, not connected to RabbitMQ, buffering events until reconnected: %s",
									msg.routingKey);
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
