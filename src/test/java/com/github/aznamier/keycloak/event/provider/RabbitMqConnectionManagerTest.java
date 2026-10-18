package com.github.aznamier.keycloak.event.provider;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.io.IOException;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.BooleanSupplier;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import com.rabbitmq.client.BlockedListener;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ExceptionHandler;
import com.rabbitmq.client.Recoverable;
import com.rabbitmq.client.RecoveryListener;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.client.impl.DefaultExceptionHandler;

class RabbitMqConnectionManagerTest {

	private static final String PROBLEM = "keycloak-to-rabbitmq CONNECTION PROBLEM";

	private final List<String> logLines = Collections.synchronizedList(new ArrayList<>());
	private final Logger logger = Logger.getLogger(RabbitMqConnectionManager.class.getName());
	private final Handler handler = new Handler() {
		private final SimpleFormatter formatter = new SimpleFormatter();

		@Override
		public void publish(LogRecord record) {
			logLines.add(record.getLevel().getName() + " " + formatter.formatMessage(record));
		}

		@Override
		public void flush() {
		}

		@Override
		public void close() {
		}
	};

	private ConnectionFactory connectionFactory;
	private Connection connection;
	private RabbitMqConnectionManager manager;

	@BeforeEach
	void setUp() throws Exception {
		logger.addHandler(handler);
		connectionFactory = mock(ConnectionFactory.class);
		when(connectionFactory.getNetworkRecoveryInterval()).thenReturn(10L);
		connection = mock(Connection.class, withSettings().extraInterfaces(Recoverable.class));
		when(connection.isOpen()).thenReturn(true);
		Channel channel = mock(Channel.class);
		when(channel.isOpen()).thenReturn(true);
		when(connection.createChannel()).thenReturn(channel);
		RabbitMqConfig cfg = new RabbitMqConfig();
		cfg.setHostUrl("rabbit");
		cfg.setPort(5672);
		manager = new RabbitMqConnectionManager(connectionFactory, cfg);
	}

	@AfterEach
	void tearDown() {
		manager.close();
		logger.removeHandler(handler);
	}

	@Test
	void connectionLossIsLoggedWithTheReason() throws Exception {
		startConnected();

		// As the client reports a socket error or missed heartbeats: the cause is attached to the signal
		ShutdownSignalException lost = new ShutdownSignalException(true, false, null, connection);
		lost.initCause(new IOException("missed heartbeats from server"));
		shutdownListener().shutdownCompleted(lost);

		assertLogged("ERROR " + PROBLEM + ": connection to RabbitMQ at rabbit:5672 lost, reconnecting every 10 ms: "
				+ "java.io.IOException: missed heartbeats from server");
	}

	@Test
	void closingTheConnectionOnPurposeIsNotAProblem() throws Exception {
		startConnected();

		shutdownListener().shutdownCompleted(new ShutdownSignalException(true, true, null, connection));

		assertEquals(0, problems().size(), String.valueOf(logLines));
	}

	@Test
	void failedReconnectAttemptsAreSummarisedAndTheRecoveryIsLogged() throws Exception {
		startConnected();
		shutdownListener().shutdownCompleted(new ShutdownSignalException(true, false, null, connection));
		ExceptionHandler recoveryErrors = exceptionHandler();

		// Within the report interval, failed attempts are only counted
		for (int i = 0; i < 4; i++) {
			recoveryErrors.handleConnectionRecoveryException(connection, new ConnectException("Connection refused"));
		}
		assertEquals(1, problems().size(), "only the loss so far: " + logLines);

		manager.stillDownReportIntervalMs = 0;
		recoveryErrors.handleConnectionRecoveryException(connection, new ConnectException("Connection refused"));
		assertLogged("ERROR " + PROBLEM + ": still not connected to RabbitMQ at rabbit:5672 after 0 s and 5 failed attempts, "
				+ "retrying every 10 ms: java.net.ConnectException: Connection refused");

		recoveryListener().handleRecovery((Recoverable) connection);
		assertLogged("INFO keycloak-to-rabbitmq reconnected to RabbitMQ at rabbit:5672 after 0 s and 5 failed attempts");
	}

	@Test
	void blockingPublishersIsLoggedUntilUnblocked() throws Exception {
		startConnected();
		BlockedListener blocked = blockedListener();

		blocked.handleBlocked("low on memory");
		blocked.handleUnblocked();

		assertLogged("ERROR " + PROBLEM + ": RabbitMQ at rabbit:5672 is blocking publishers: low on memory");
		assertLogged("INFO keycloak-to-rabbitmq RabbitMQ at rabbit:5672 unblocked publishers after 0 s");
	}

	@Test
	void failedAttemptsAtStartupAreLoggedOnceThenSummarised() throws Exception {
		when(connectionFactory.newConnection(anyList()))
				.thenThrow(new ConnectException("Connection refused"))
				.thenThrow(new ConnectException("Connection refused"))
				.thenReturn(connection);

		manager.start();
		await(() -> manager.getChannel() != null);

		assertEquals(1, problems().size(), "the first failure only: " + logLines);
		assertLogged("ERROR " + PROBLEM + ": could not connect to RabbitMQ at rabbit:5672, retrying every 10 ms");
		assertLogged("INFO keycloak-to-rabbitmq connected to RabbitMQ at rabbit:5672 after 0 s and 2 failed attempts");
	}

	@Test
	void theClientsConnectionDriverErrorsAreOnlyLoggedForDebugging() throws Exception {
		Level level = logger.getLevel();
		logger.setLevel(Level.ALL);
		try {
			exceptionHandler().handleUnexpectedConnectionDriverException(connection, new IOException("Socket closed"));
		} finally {
			logger.setLevel(level);
		}

		assertLogged("DEBUG keycloak-to-rabbitmq connection driver error on RabbitMQ at rabbit:5672");
		assertEquals(1, logLines.size(), String.valueOf(logLines));
	}

	@Test
	void theClientDoesNotAlsoLogConnectionDriverErrors() {
		List<String> clientLines = Collections.synchronizedList(new ArrayList<>());
		// The client logs through SLF4J, which Keycloak's JBoss LogManager binding receives
		Logger clientLogger = org.jboss.logmanager.Logger.getLogger("com.rabbitmq");
		Handler clientHandler = new Handler() {
			@Override
			public void publish(LogRecord record) {
				clientLines.add(record.getLevel().getName() + " " + record.getMessage());
			}

			@Override
			public void flush() {
			}

			@Override
			public void close() {
			}
		};
		clientLogger.addHandler(clientHandler);
		try {
			// The client's own handler logs the error, so the capture works
			new DefaultExceptionHandler().handleUnexpectedConnectionDriverException(connection, new IOException("Socket closed"));
			assertEquals(1, clientLines.size(), "the client's default handler should log it: " + clientLines);

			clientLines.clear();
			exceptionHandler().handleUnexpectedConnectionDriverException(connection, new IOException("Socket closed"));
			assertTrue(clientLines.isEmpty(), "the client logged it too: " + clientLines);
		} finally {
			clientLogger.removeHandler(clientHandler);
		}
	}

	@Test
	void durationsAreReadable() {
		assertEquals("45 s", RabbitMqConnectionManager.duration(45_400));
		assertEquals("2 m 5 s", RabbitMqConnectionManager.duration(125_000));
		assertEquals("1 h 2 m", RabbitMqConnectionManager.duration(3_725_000));
	}

	private void startConnected() throws Exception {
		when(connectionFactory.newConnection(anyList())).thenReturn(connection);
		manager.start();
		await(() -> manager.getChannel() != null);
		assertLogged("INFO keycloak-to-rabbitmq connected to RabbitMQ at rabbit:5672");
	}

	private ShutdownListener shutdownListener() {
		ArgumentCaptor<ShutdownListener> captor = ArgumentCaptor.forClass(ShutdownListener.class);
		verify(connection).addShutdownListener(captor.capture());
		return captor.getValue();
	}

	private BlockedListener blockedListener() {
		ArgumentCaptor<BlockedListener> captor = ArgumentCaptor.forClass(BlockedListener.class);
		verify(connection).addBlockedListener(captor.capture());
		return captor.getValue();
	}

	private RecoveryListener recoveryListener() {
		ArgumentCaptor<RecoveryListener> captor = ArgumentCaptor.forClass(RecoveryListener.class);
		verify((Recoverable) connection).addRecoveryListener(captor.capture());
		return captor.getValue();
	}

	private ExceptionHandler exceptionHandler() {
		ArgumentCaptor<ExceptionHandler> captor = ArgumentCaptor.forClass(ExceptionHandler.class);
		verify(connectionFactory).setExceptionHandler(captor.capture());
		return captor.getValue();
	}

	private List<String> problems() {
		synchronized (logLines) {
			return logLines.stream().filter(l -> l.contains(PROBLEM)).collect(Collectors.toList());
		}
	}

	private void assertLogged(String line) {
		synchronized (logLines) {
			assertTrue(logLines.stream().anyMatch(l -> l.equals(line)), "expected log line:\n  " + line + "\nbut got:\n  "
					+ String.join("\n  ", logLines));
		}
	}

	private static void await(BooleanSupplier condition) throws InterruptedException {
		long deadline = System.currentTimeMillis() + 5_000;
		while (!condition.getAsBoolean()) {
			if (System.currentTimeMillis() > deadline) {
				throw new AssertionError("condition not met within 5s");
			}
			Thread.sleep(10);
		}
	}
}
