package com.github.aznamier.keycloak.event.provider;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.jboss.logging.Logger;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * Keeps a single long-lived connection and channel to RabbitMQ.
 *
 * Connecting only ever happens on a background thread, so Keycloak requests never wait on the broker.
 * The first connection is opened at startup and retried until it succeeds; after that, the client's
 * automatic recovery reconnects the same connection and channel after a drop.
 */
public class RabbitMqConnectionManager {

	private static final Logger log = Logger.getLogger(RabbitMqConnectionManager.class);

	private static final int CLOSE_TIMEOUT_MS = 5000;

	private final ConnectionFactory connectionFactory;
	private final RabbitMqConfig cfg;
	private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(r -> {
		Thread t = new Thread(r, "keycloak-to-rabbitmq-connect");
		t.setDaemon(true);
		return t;
	});
	private final AtomicBoolean connectScheduled = new AtomicBoolean();

	private volatile Connection connection;
	private volatile Channel channel;
	private volatile boolean closed;
	// Only used on the connect thread
	private int failedAttempts;

	public RabbitMqConnectionManager(ConnectionFactory connectionFactory, RabbitMqConfig cfg) {
		this.connectionFactory = connectionFactory;
		this.cfg = cfg;
	}

	public void start() {
		scheduleConnect(0);
	}

	/**
	 * Returns the open channel, or null if there is none right now. Never blocks: if the channel
	 * is missing it schedules a background attempt to open it.
	 */
	public Channel getChannel() {
		Channel ch = channel;
		if (ch != null && ch.isOpen()) {
			return ch;
		}
		scheduleConnect(0);
		return null;
	}

	public void close() {
		closed = true;
		executor.shutdownNow();
		Connection conn = connection;
		if (conn != null) {
			try {
				if (conn.isOpen()) {
					conn.close(CLOSE_TIMEOUT_MS);
				} else {
					// Disconnected or recovering: stop recovery without waiting on the broker
					conn.abort(CLOSE_TIMEOUT_MS);
				}
			} catch (Exception e) {
				log.error("keycloak-to-rabbitmq ERROR on close", e);
			}
		}
	}

	private void scheduleConnect(long delayMs) {
		if (closed || !connectScheduled.compareAndSet(false, true)) {
			return;
		}
		try {
			executor.schedule(this::connect, delayMs, TimeUnit.MILLISECONDS);
		} catch (Exception e) {
			// Executor already shut down
			connectScheduled.set(false);
		}
	}

	private void connect() {
		connectScheduled.set(false);
		try {
			Connection conn = connection;
			if (conn == null) {
				if (failedAttempts == 0) {
					log.infof("keycloak-to-rabbitmq connecting to RabbitMQ at %s:%d", cfg.getHostUrl(), cfg.getPort());
				}
				conn = connectionFactory.newConnection();
				connection = conn;
				if (failedAttempts == 0) {
					log.infof("keycloak-to-rabbitmq connected to RabbitMQ at %s:%d", cfg.getHostUrl(), cfg.getPort());
				} else {
					log.infof("keycloak-to-rabbitmq connected to RabbitMQ at %s:%d after %d failed attempts",
							cfg.getHostUrl(), cfg.getPort(), failedAttempts);
				}
				failedAttempts = 0;
			} else if (!conn.isOpen()) {
				// Automatic recovery is reconnecting; it restores the channel too
				return;
			}
			Channel ch = channel;
			if (ch == null || !ch.isOpen()) {
				// Also covers a channel closed by a channel-level error, which automatic recovery does not reopen
				channel = conn.createChannel();
			}
		} catch (IOException | TimeoutException e) {
			long retryMs = connectionFactory.getNetworkRecoveryInterval();
			failedAttempts++;
			// Full stack trace only on the first failure, so a long outage doesn't flood the logs
			if (failedAttempts == 1) {
				log.errorf(e, "keycloak-to-rabbitmq ERROR on connection to RabbitMQ at %s:%d, retrying every %d ms",
						cfg.getHostUrl(), cfg.getPort(), retryMs);
			} else {
				log.errorf("keycloak-to-rabbitmq ERROR on connection to RabbitMQ at %s:%d (attempt %d), retrying in %d ms: %s",
						cfg.getHostUrl(), cfg.getPort(), failedAttempts, retryMs, e);
			}
			scheduleConnect(retryMs);
		}
	}
}
