package com.github.aznamier.keycloak.event.provider;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.keycloak.Config.Scope;

class RabbitMqEventListenerProviderFactoryTest {

	@Test
	void aTlsSetupThatFailsStopsStartupInsteadOfConnectingWithoutTls() {
		Map<String, String> settings = new HashMap<>();
		settings.put("use_tls", "true");
		settings.put("trust_store", "/does/not/exist.jks");

		IllegalStateException e = assertThrows(IllegalStateException.class, () -> initFactory(settings));

		assertTrue(e.getMessage().startsWith("keycloak-to-rabbitmq could not set up TLS for RabbitMQ"), e.getMessage());
	}

	private static void initFactory(Map<String, String> settings) {
		Scope scope = mock(Scope.class);
		when(scope.get(anyString())).thenAnswer(i -> settings.get(i.<String>getArgument(0)));
		new RabbitMqEventListenerProviderFactory().init(scope);
	}
}
