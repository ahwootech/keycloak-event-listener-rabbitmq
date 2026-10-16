package com.github.aznamier.keycloak.event.provider;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.keycloak.Config.Scope;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

/**
 * Connects the real plugin factory to a RabbitMQ with TLS, using a test CA and a server certificate for localhost.
 */
@Testcontainers(disabledWithoutDocker = true)
class RabbitMqTlsIT {

	@Container
	private static final RabbitMQContainer rabbitmq = new RabbitMQContainer("rabbitmq:4.1-alpine")
			.withCopyFileToContainer(MountableFile.forClasspathResource("tls/ca.pem", 0644), "/etc/rabbitmq/tls/ca.pem")
			.withCopyFileToContainer(MountableFile.forClasspathResource("tls/server.pem", 0644), "/etc/rabbitmq/tls/server.pem")
			.withCopyFileToContainer(MountableFile.forClasspathResource("tls/server.key", 0644), "/etc/rabbitmq/tls/server.key")
			.withCopyFileToContainer(MountableFile.forClasspathResource("tls/rabbitmq-tls.conf", 0644), "/etc/rabbitmq/conf.d/20-tls.conf");

	private final Map<String, String> settings = new HashMap<>();
	private final List<LogRecord> logRecords = Collections.synchronizedList(new ArrayList<>());
	private final Logger managerLogger = Logger.getLogger(RabbitMqConnectionManager.class.getName());
	private final Handler logHandler = new Handler() {
		@Override
		public void publish(LogRecord record) {
			logRecords.add(record);
		}

		@Override
		public void flush() {
		}

		@Override
		public void close() {
		}
	};
	private RabbitMqEventListenerProviderFactory factory;

	@BeforeEach
	void setUp() {
		managerLogger.addHandler(logHandler);
		settings.put("url", "localhost");
		settings.put("port", String.valueOf(rabbitmq.getAmqpsPort()));
		settings.put("username", rabbitmq.getAdminUsername());
		settings.put("password", rabbitmq.getAdminPassword());
		settings.put("vhost", "/");
		settings.put("use_tls", "true");
	}

	@AfterEach
	void tearDown() {
		if (factory != null) {
			factory.close();
		}
		managerLogger.removeHandler(logHandler);
	}

	@Test
	void connectsWhenTheTruststoreTrustsRabbitMqsCertificate() throws Exception {
		settings.put("trust_store", new File(getClass().getResource("/tls/truststore.jks").toURI()).getAbsolutePath());
		settings.put("trust_store_pass", "changeit");
		RabbitMqConnectionManager manager = startPlugin();

		await(() -> manager.getChannel() != null);
	}

	@Test
	void refusesACertificateTheJvmDoesNotTrust() throws Exception {
		// No truststore: the JVM's trusted certificates, which don't include the test CA
		RabbitMqConnectionManager manager = startPlugin();

		// Either the connection fails, or (wrongly) it connects
		await(() -> manager.getChannel() != null || logRecords.stream().anyMatch(r -> r.getThrown() != null));
		assertNull(manager.getChannel(), "connected to a server with an untrusted certificate");
		Throwable thrown = logRecords.stream().filter(r -> r.getThrown() != null).findFirst().get().getThrown();
		String causes = causes(thrown);
		assertTrue(causes.contains("PKIX path building failed") || causes.contains("SSLHandshakeException"), causes);
	}

	@Test
	void withVerificationOffItConnectsDespiteAnUntrustedCertificate() throws Exception {
		// Encryption only, e.g. a self-signed certificate on a private network
		settings.put("tls_verify", "false");
		RabbitMqConnectionManager manager = startPlugin();

		await(() -> manager.getChannel() != null);
	}

	private RabbitMqConnectionManager startPlugin() throws Exception {
		Scope scope = mock(Scope.class);
		when(scope.get(anyString())).thenAnswer(i -> settings.get(i.<String>getArgument(0)));
		factory = new RabbitMqEventListenerProviderFactory();
		factory.init(scope);
		factory.postInit(null);
		Field field = RabbitMqEventListenerProviderFactory.class.getDeclaredField("connectionManager");
		field.setAccessible(true);
		return (RabbitMqConnectionManager) field.get(factory);
	}

	private static String causes(Throwable t) {
		StringBuilder out = new StringBuilder();
		for (Throwable c = t; c != null; c = c.getCause()) {
			out.append(c).append('\n');
		}
		return out.toString();
	}

	private static void await(BooleanSupplier condition) throws InterruptedException {
		long deadline = System.currentTimeMillis() + 30_000;
		while (!condition.getAsBoolean()) {
			if (System.currentTimeMillis() > deadline) {
				throw new AssertionError("timed out");
			}
			Thread.sleep(50);
		}
	}
}
