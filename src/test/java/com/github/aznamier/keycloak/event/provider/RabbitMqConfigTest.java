package com.github.aznamier.keycloak.event.provider;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.keycloak.Config.Scope;

class RabbitMqConfigTest {

	@Test
	void defaults() {
		RabbitMqConfig cfg = RabbitMqConfig.createFromScope(null);

		assertEquals(5672, cfg.getPort());
		assertFalse(cfg.getUseTls());
		assertTrue(cfg.getTlsVerify(), "TLS is verified unless turned off");
		assertEquals(60, cfg.getHeartbeatSeconds());
		assertNull(cfg.getConnectionTimeoutMs(), "the client's defaults");
		assertEquals(5000L, cfg.getNetworkRecoveryIntervalMs());
	}

	@Test
	void anInvalidNumberStopsStartupNamingTheSetting() {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> fromSettings("port", "5672x"));

		assertEquals("keycloak-to-rabbitmq setting port (KK_TO_RMQ_PORT) must be a whole number from 1 to 65535, got '5672x'",
				e.getMessage());
	}

	@Test
	void anInvalidHeartbeatStopsStartup() {
		assertEquals("keycloak-to-rabbitmq setting heartbeat_seconds (KK_TO_RMQ_HEARTBEAT_SECONDS) must be a whole number of at least 0, got '10s'",
				assertThrows(IllegalArgumentException.class, () -> fromSettings("heartbeat_seconds", "10s")).getMessage());
	}

	@Test
	void numbersOutOfRangeStopStartup() {
		assertEquals("keycloak-to-rabbitmq setting port (KK_TO_RMQ_PORT) must be a whole number from 1 to 65535, got '70000'",
				assertThrows(IllegalArgumentException.class, () -> fromSettings("port", "70000")).getMessage());
		assertThrows(IllegalArgumentException.class, () -> fromSettings("port", "0"));
	}

	@Test
	void aTrueFalseSettingMustBeTrueOrFalse() {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> fromSettings("use_tls", "yes"));

		assertEquals("keycloak-to-rabbitmq setting use_tls (KK_TO_RMQ_USE_TLS) must be true or false, got 'yes'", e.getMessage());
	}

	@Test
	void surroundingSpacesAndCapitalsAreAccepted() {
		assertEquals(5673, fromSettings("port", " 5673 ").getPort());
		assertTrue(fromSettings("use_tls", " TRUE ").getUseTls());
	}

	private static RabbitMqConfig fromSettings(String name, String value) {
		Map<String, String> settings = new HashMap<>();
		settings.put(name, value);
		Scope scope = mock(Scope.class);
		when(scope.get(anyString())).thenAnswer(i -> settings.get(i.<String>getArgument(0)));
		return RabbitMqConfig.createFromScope(scope);
	}
}
