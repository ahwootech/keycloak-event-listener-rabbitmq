package com.github.aznamier.keycloak.event.provider;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.jboss.logging.Logger;

import com.rabbitmq.client.BlockedListener;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Recoverable;
import com.rabbitmq.client.RecoveryListener;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.client.impl.DefaultExceptionHandler;

/**
 * Keeps a single long-lived connection and channel to RabbitMQ.
 *
 * Connecting only ever happens on a background thread, so Keycloak requests never wait on the broker.
 * The first connection is opened at startup and retried until it succeeds; after that, the client's
 * automatic recovery reconnects the same connection and channel after a drop.
 *
 * When essential events are configured, a second channel in confirm mode is kept for them, so they are
 * never held up by (or confused with) messages published on the other channel.
 *
 * Losing the connection, failing to reconnect, and RabbitMQ blocking publishers are logged with the
 * {@link #CONNECTION_PROBLEM} prefix as soon as they happen, whether or not any events are being sent.
 *
 * Internal to the plugin, so it can change in any release.
 */
class RabbitMqConnectionManager {

	private static final Logger log = Logger.getLogger(RabbitMqConnectionManager.class);

	// Prefix for every log line about RabbitMQ being unreachable or blocking publishers, so it can be alerted on
	static final String CONNECTION_PROBLEM = "keycloak-to-rabbitmq CONNECTION PROBLEM";

	private static final int CLOSE_TIMEOUT_MS = 5000;
	private static final long STILL_DOWN_REPORT_INTERVAL_MS = 60_000;

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
	private volatile Channel essentialChannel;
	// messageIds of mandatory messages RabbitMQ returned as unroutable on the essential channel
	private final Set<String> returnedMessageIds = ConcurrentHashMap.newKeySet();
	private volatile boolean closed;

	// Connection state, for logging; guarded by this. The first connection at startup and automatic recovery
	// after a drop are reported the same way.
	private boolean everConnected;
	// When the connection was lost (or the first attempt at startup failed); 0 while connected
	private long downSince;
	private int failedAttempts;
	private long lastStillDownReport;
	private long blockedSince;
	// Package-private so tests don't have to wait a minute
	long stillDownReportIntervalMs = STILL_DOWN_REPORT_INTERVAL_MS;

	RabbitMqConnectionManager(ConnectionFactory connectionFactory, RabbitMqConfig cfg) {
		this.connectionFactory = connectionFactory;
		this.cfg = cfg;
		// Failed reconnect attempts by automatic recovery are reported here, at most once a minute,
		// instead of the client logging every attempt with a stack trace
		connectionFactory.setExceptionHandler(new DefaultExceptionHandler() {
			@Override
			public void handleConnectionRecoveryException(Connection conn, Throwable exception) {
				attemptFailed(exception);
			}

			@Override
			public void handleUnexpectedConnectionDriverException(Connection conn, Throwable exception) {
				// The client logs this on every lost connection and failed reconnect attempt; the CONNECTION PROBLEM
				// lines already report them, so it is only kept for debugging
				log.debugf(exception, "keycloak-to-rabbitmq connection driver error on RabbitMQ at %s", cfg.describeAddresses());
			}
		});
	}

	void start() {
		scheduleConnect(0);
	}

	/**
	 * Returns the open channel, or null if there is none right now. Never blocks: if the channel
	 * is missing it schedules a background attempt to open it.
	 */
	Channel getChannel() {
		Channel ch = channel;
		if (ch != null && ch.isOpen()) {
			return ch;
		}
		scheduleConnect(0);
		return null;
	}

	/**
	 * Like {@link #getChannel()}, for the essential events channel (in confirm mode).
	 */
	Channel getEssentialChannel() {
		Channel ch = essentialChannel;
		if (ch != null && ch.isOpen()) {
			return ch;
		}
		scheduleConnect(0);
		return null;
	}

	/**
	 * Returns true, once, if RabbitMQ returned the mandatory message with this messageId as unroutable.
	 */
	boolean consumeReturned(String messageId) {
		return messageId != null && returnedMessageIds.remove(messageId);
	}

	void close() {
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
				if (isFirstAttempt()) {
					log.infof("keycloak-to-rabbitmq connecting to RabbitMQ at %s", cfg.describeAddresses());
				}
				conn = connectionFactory.newConnection(cfg.getAddresses());
				watch(conn);
				connection = conn;
				connected();
			} else if (!conn.isOpen()) {
				// Automatic recovery is reconnecting; it restores the channel too
				return;
			}
			Channel ch = channel;
			if (ch == null || !ch.isOpen()) {
				// Also covers a channel closed by a channel-level error, which automatic recovery does not reopen
				ch = conn.createChannel();
				if (RabbitMqPublisher.confirmsEnabled(cfg)) {
					// Automatic recovery restores confirm mode on the recovered channel
					ch.confirmSelect();
				}
				channel = ch;
			}
			if (!cfg.getEssentialEvents().isEmpty()) {
				Channel ech = essentialChannel;
				if (ech == null || !ech.isOpen()) {
					ech = conn.createChannel();
					// Automatic recovery restores confirm mode and the return listener on the recovered channel
					ech.confirmSelect();
					ech.addReturnListener(r -> {
						String messageId = r.getProperties() != null ? r.getProperties().getMessageId() : null;
						if (messageId != null) {
							returnedMessageIds.add(messageId);
						}
					});
					essentialChannel = ech;
				}
			}
		} catch (IOException | TimeoutException e) {
			if (connection == null) {
				attemptFailed(e);
			} else {
				log.errorf(e, "keycloak-to-rabbitmq ERROR opening a channel to RabbitMQ at %s, retrying in %d ms",
						cfg.describeAddresses(), connectionFactory.getNetworkRecoveryInterval());
			}
			scheduleConnect(connectionFactory.getNetworkRecoveryInterval());
		}
	}

	/**
	 * Logs the connection being lost, recovered or blocked. The listeners stay registered when automatic recovery
	 * replaces the underlying connection.
	 */
	private void watch(Connection conn) {
		conn.addShutdownListener(cause -> {
			if (!closed && !cause.isInitiatedByApplication()) {
				connectionLost(cause);
			}
		});
		conn.addBlockedListener(new BlockedListener() {
			@Override
			public void handleBlocked(String reason) {
				blocked(reason);
			}

			@Override
			public void handleUnblocked() {
				unblocked();
			}
		});
		if (conn instanceof Recoverable) {
			((Recoverable) conn).addRecoveryListener(new RecoveryListener() {
				@Override
				public void handleRecovery(Recoverable recoverable) {
					connected();
				}

				@Override
				public void handleRecoveryStarted(Recoverable recoverable) {
					// Logged when the connection is lost
				}
			});
		}
	}

	private synchronized boolean isFirstAttempt() {
		return !everConnected && failedAttempts == 0;
	}

	private synchronized void connectionLost(ShutdownSignalException cause) {
		long now = System.currentTimeMillis();
		downSince = now;
		lastStillDownReport = now;
		failedAttempts = 0;
		blockedSince = 0;
		log.errorf("%s: connection to RabbitMQ at %s lost, reconnecting every %d ms: %s", CONNECTION_PROBLEM,
				cfg.describeAddresses(), connectionFactory.getNetworkRecoveryInterval(), describe(cause));
	}

	/**
	 * A connection attempt failed, at startup or during automatic recovery. The first failure at startup is logged
	 * with its stack trace; after that, a summary at most once a minute, so a long outage doesn't flood the logs.
	 */
	private synchronized void attemptFailed(Throwable e) {
		long now = System.currentTimeMillis();
		failedAttempts++;
		if (downSince == 0) {
			downSince = now;
			lastStillDownReport = now;
			if (!everConnected) {
				log.errorf(e, "%s: could not connect to RabbitMQ at %s, retrying every %d ms", CONNECTION_PROBLEM,
						cfg.describeAddresses(), connectionFactory.getNetworkRecoveryInterval());
			}
			return;
		}
		if (now - lastStillDownReport >= stillDownReportIntervalMs) {
			lastStillDownReport = now;
			log.errorf("%s: still not connected to RabbitMQ at %s after %s and %s, retrying every %d ms: %s",
					CONNECTION_PROBLEM, cfg.describeAddresses(), duration(now - downSince), failedAttempts(),
					connectionFactory.getNetworkRecoveryInterval(), e);
		}
	}

	private synchronized void connected() {
		if (downSince == 0) {
			log.infof("keycloak-to-rabbitmq connected to RabbitMQ at %s", cfg.describeAddresses());
		} else {
			log.infof("keycloak-to-rabbitmq %s to RabbitMQ at %s after %s and %s",
					everConnected ? "reconnected" : "connected", cfg.describeAddresses(),
					duration(System.currentTimeMillis() - downSince), failedAttempts());
		}
		everConnected = true;
		downSince = 0;
		failedAttempts = 0;
	}

	private synchronized void blocked(String reason) {
		blockedSince = System.currentTimeMillis();
		log.errorf("%s: RabbitMQ at %s is blocking publishers: %s", CONNECTION_PROBLEM, cfg.describeAddresses(), reason);
	}

	private synchronized void unblocked() {
		if (blockedSince != 0) {
			log.infof("keycloak-to-rabbitmq RabbitMQ at %s unblocked publishers after %s", cfg.describeAddresses(),
					duration(System.currentTimeMillis() - blockedSince));
		}
		blockedSince = 0;
	}

	private String failedAttempts() {
		return failedAttempts + (failedAttempts == 1 ? " failed attempt" : " failed attempts");
	}

	private static String describe(ShutdownSignalException cause) {
		// A missed heartbeat or a socket error is the cause; a close by the broker is described by the signal itself
		return cause.getCause() != null ? cause.getCause().toString() : cause.getMessage();
	}

	static String duration(long ms) {
		long s = ms / 1000;
		if (s < 60) {
			return s + " s";
		}
		if (s < 3600) {
			return (s / 60) + " m " + (s % 60) + " s";
		}
		return (s / 3600) + " h " + (s % 3600 / 60) + " m";
	}
}
