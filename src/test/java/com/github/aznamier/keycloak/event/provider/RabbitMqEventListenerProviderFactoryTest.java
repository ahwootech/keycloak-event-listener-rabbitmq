package com.github.aznamier.keycloak.event.provider;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.keycloak.Config.Scope;

import com.rabbitmq.client.ConnectionFactory;

class RabbitMqEventListenerProviderFactoryTest {

	@Test
	void connectionTimeoutAppliesToTheTcpConnectionAndTheHandshake() throws Exception {
		Map<String, String> settings = new HashMap<>();
		settings.put("connection_timeout_ms", "5000");

		ConnectionFactory cf = initFactory(settings);

		assertEquals(5000, cf.getConnectionTimeout());
		assertEquals(5000, cf.getHandshakeTimeout());
	}

	@Test
	void unsetConnectionTimeoutKeepsTheClientDefaults() throws Exception {
		ConnectionFactory cf = initFactory(new HashMap<>());

		assertEquals(ConnectionFactory.DEFAULT_CONNECTION_TIMEOUT, cf.getConnectionTimeout());
		assertEquals(ConnectionFactory.DEFAULT_HANDSHAKE_TIMEOUT, cf.getHandshakeTimeout());
	}

	@Test
	void aTlsSetupThatFailsStopsStartupInsteadOfConnectingWithoutTls() {
		Map<String, String> settings = new HashMap<>();
		settings.put("use_tls", "true");
		settings.put("trust_store", "/does/not/exist.jks");

		IllegalStateException e = assertThrows(IllegalStateException.class, () -> initFactory(settings));

		assertTrue(e.getMessage().startsWith("keycloak-to-rabbitmq could not set up TLS for RabbitMQ"), e.getMessage());
	}

	private static ConnectionFactory initFactory(Map<String, String> settings) throws Exception {
		settings.putIfAbsent("hosts", "localhost");
		settings.putIfAbsent("vhost", "/");
		Scope scope = mock(Scope.class);
		when(scope.get(anyString())).thenAnswer(i -> settings.get(i.<String>getArgument(0)));
		RabbitMqEventListenerProviderFactory factory = new RabbitMqEventListenerProviderFactory();
		factory.init(scope);
		Field field = RabbitMqEventListenerProviderFactory.class.getDeclaredField("connectionFactory");
		field.setAccessible(true);
		return (ConnectionFactory) field.get(factory);
	}
}
