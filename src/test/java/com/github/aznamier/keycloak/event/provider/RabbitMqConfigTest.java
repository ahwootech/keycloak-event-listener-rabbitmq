package com.github.aznamier.keycloak.event.provider;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.junit.jupiter.api.Test;
import org.keycloak.Config.Scope;
import org.keycloak.events.Details;
import org.keycloak.events.Event;
import org.keycloak.events.EventType;
import org.keycloak.events.admin.AdminEvent;
import org.keycloak.events.admin.OperationType;
import org.keycloak.models.KeycloakSession;

class RabbitMqConfigTest {

	@Test
	void defaults() {
		RabbitMqConfig cfg = fromSettings(new HashMap<>());

		assertEquals(5672, cfg.getPort());
		assertFalse(cfg.getUseTls());
		assertTrue(cfg.getTlsVerify(), "TLS is verified unless turned off");
		assertEquals(60, cfg.getHeartbeatSeconds());
		assertNull(cfg.getConnectionTimeoutMs(), "the client's defaults");
		assertEquals(5000L, cfg.getNetworkRecoveryIntervalMs());
		assertEquals(RabbitMqConfig.PublishMode.SYNC, cfg.getPublishMode(), "events are sent on the request thread unless turned on");
		assertEquals(100, cfg.getPublishBufferMaxMb());
		assertEquals(5000L, cfg.getShutdownFlushTimeoutMs());
		assertEquals(5000L, cfg.getConfirmTimeoutMs());
	}

	@Test
	void keycloakOptionsCanUseHyphensOrUnderscores() {
		Map<String, String> settings = new HashMap<>();
		settings.put("heartbeat-seconds", "10");
		settings.put("tls_verify", "false");

		RabbitMqConfig cfg = fromSettings(settings);

		assertEquals(10, cfg.getHeartbeatSeconds());
		assertFalse(cfg.getTlsVerify());
	}

	@Test
	void secretsAreNotLogged() {
		Map<String, String> settings = new HashMap<>();
		settings.put("password", "rabbit-secret");
		settings.put("key_store_pass", "key-secret");
		settings.put("trust_store_pass", "");
		List<String> lines = new ArrayList<>();
		Handler handler = new Handler() {
			@Override
			public void publish(LogRecord record) {
				lines.add(new SimpleFormatter().formatMessage(record).trim());
			}

			@Override
			public void flush() {
			}

			@Override
			public void close() {
			}
		};
		Logger logger = Logger.getLogger(RabbitMqConfig.class.getName());
		logger.addHandler(handler);
		try {
			RabbitMqConfig cfg = fromSettings(settings);
			assertEquals("key-secret", cfg.getKeytStorePass());
		} finally {
			logger.removeHandler(handler);
		}

		assertFalse(lines.stream().anyMatch(l -> l.contains("secret")), String.join("\n", lines));
		assertTrue(lines.contains("keycloak-to-rabbitmq configuration: password=<hidden>"), String.join("\n", lines));
		assertTrue(lines.contains("keycloak-to-rabbitmq configuration: key_store_pass=<hidden>"));
		assertTrue(lines.contains("keycloak-to-rabbitmq configuration: trust_store_pass="), "an empty secret shows as not set");
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
		assertThrows(IllegalArgumentException.class, () -> fromSettings("publish_buffer_max_mb", "0"));
	}

	@Test
	void aTrueFalseSettingMustBeTrueOrFalse() {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> fromSettings("use_tls", "yes"));

		assertEquals("keycloak-to-rabbitmq setting use_tls (KK_TO_RMQ_USE_TLS) must be true or false, got 'yes'", e.getMessage());
	}

	@Test
	void aMissingVhostStopsStartup() {
		Map<String, String> settings = new HashMap<>();
		settings.put("vhost", null);
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> fromSettings(settings));

		assertEquals("keycloak-to-rabbitmq setting vhost (KK_TO_RMQ_VHOST) must be set, e.g. to / for RabbitMQ's default virtual host, got ''",
				e.getMessage());
		assertThrows(IllegalArgumentException.class, () -> fromSettings("vhost", " "));
		assertEquals("keycloak-events", fromSettings("vhost", "keycloak-events").getVhost());
	}

	@Test
	void urlListsOneOrMoreHostsEachWithAnOptionalPort() {
		Map<String, String> settings = new HashMap<>();
		settings.put("url", "rabbit-1, rabbit-2:5673,,[::1]:5674, [fe80::1], rabbit-3");
		settings.put("port", "5671");

		assertEquals("rabbit-1:5671, rabbit-2:5673, [::1]:5674, [fe80::1]:5671, rabbit-3:5671", fromSettings(settings).describeAddresses());
		assertEquals("localhost:5672", fromSettings(new HashMap<>()).describeAddresses());
		assertEquals("rabbit:5673", fromSettings("url", "rabbit:5673").describeAddresses());
	}

	@Test
	void anInvalidUrlStopsStartup() {
		assertEquals("keycloak-to-rabbitmq setting url (KK_TO_RMQ_URL) must be one or more comma-separated hosts, each optionally with :port (IPv6 addresses in brackets), got 'rabbit-1,rabbit-2:abc'",
				assertThrows(IllegalArgumentException.class, () -> fromSettings("url", "rabbit-1,rabbit-2:abc")).getMessage());
		assertThrows(IllegalArgumentException.class, () -> fromSettings("url", "rabbit:70000"));
		assertThrows(IllegalArgumentException.class, () -> fromSettings("url", " , "));
		assertThrows(IllegalArgumentException.class, () -> fromSettings("url", "fe80::1"), "IPv6 needs brackets");
	}

	@Test
	void publishModeIsOneOfTheKnownModes() {
		assertEquals(RabbitMqConfig.PublishMode.ASYNC, fromSettings("publish_mode", " Async ").getPublishMode());
		assertEquals(RabbitMqConfig.PublishMode.CONFIRMED, fromSettings("publish_mode", "confirmed").getPublishMode());
		assertEquals("keycloak-to-rabbitmq setting publish_mode (KK_TO_RMQ_PUBLISH_MODE) must be one of sync, async, confirmed, got 'background'",
				assertThrows(IllegalArgumentException.class, () -> fromSettings("publish_mode", "background")).getMessage());
	}

	@Test
	void surroundingSpacesAndCapitalsAreAccepted() {
		assertEquals(5673, fromSettings("port", " 5673 ").getPort());
		assertTrue(fromSettings("use_tls", " TRUE ").getUseTls());
	}

	@Test
	void routingKeyForClientEvents() {
		KeycloakSession session = mock(KeycloakSession.class, RETURNS_DEEP_STUBS);
		when(session.realms().getRealm("realm-id").getName()).thenReturn("My.Realm");
		Event event = event(EventType.LOGIN, null, null);

		assertEquals("KK.EVENT.CLIENT.MyRealm.SUCCESS.my-client.LOGIN", RabbitMqConfig.calculateRoutingKey(event, session));
	}

	@Test
	void routingKeyForAdminEvents() {
		KeycloakSession session = mock(KeycloakSession.class, RETURNS_DEEP_STUBS);
		when(session.getContext().getRealm().getName()).thenReturn("my realm");

		assertEquals("KK.EVENT.ADMIN.my_realm.ERROR.USER.DELETE",
				RabbitMqConfig.calculateRoutingKey(adminEvent("USER", OperationType.DELETE, "failed"), session));
	}

	@Test
	void normalizeKeyRemovesSpecialCharactersAndReplacesSpaces() {
		assertEquals("KK.EVENT.a_b.c-d", RabbitMqConfig.normalizeKey("KK.EVENT.a b.c-d!@$%"));
		assertEquals("abc", RabbitMqConfig.removeDots("a.b.c"));
	}

	private static RabbitMqConfig fromSettings(String name, String value) {
		Map<String, String> settings = new HashMap<>();
		settings.put(name, value);
		return fromSettings(settings);
	}

	/** The plugin's configuration from these Keycloak options, with vhost "/" unless they set it (even to null). */
	private static RabbitMqConfig fromSettings(Map<String, String> settings) {
		Map<String, String> withVhost = new HashMap<>(settings);
		if (!settings.containsKey("vhost")) {
			withVhost.put("vhost", "/");
		}
		Scope scope = mock(Scope.class);
		when(scope.get(anyString())).thenAnswer(i -> withVhost.get(i.<String>getArgument(0)));
		return RabbitMqConfig.createFromScope(scope);
	}

	private static Event event(EventType type, String error, String registerMethod) {
		Event event = new Event();
		event.setType(type);
		event.setError(error);
		event.setRealmId("realm-id");
		event.setClientId("my-client");
		if (registerMethod != null) {
			Map<String, String> details = new HashMap<>();
			details.put(Details.REGISTER_METHOD, registerMethod);
			event.setDetails(details);
		}
		return event;
	}

	private static AdminEvent adminEvent(String resourceType, OperationType operation, String error) {
		AdminEvent event = new AdminEvent();
		event.setResourceTypeAsString(resourceType);
		event.setOperationType(operation);
		event.setError(error);
		return event;
	}
}
