package com.github.aznamier.keycloak.event.provider;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.keycloak.events.Event;
import org.keycloak.events.EventType;
import org.keycloak.events.admin.AdminEvent;
import org.keycloak.events.admin.OperationType;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakTransaction;
import org.mockito.ArgumentCaptor;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;

/**
 * The provider skips excluded events before doing anything else with them.
 */
class RabbitMqExcludedEventsTest {

	private KeycloakSession session;
	private Channel channel;
	private RabbitMqConfig cfg;

	@BeforeEach
	void setUp() {
		session = mock(KeycloakSession.class, RETURNS_DEEP_STUBS);
		when(session.realms().getRealm("realm-id").getName()).thenReturn("test");
		when(session.getContext().getRealm().getName()).thenReturn("test");
		channel = mock(Channel.class);
		cfg = new RabbitMqConfig();
		cfg.setExchange("amq.topic");
		cfg.setExcludedEvents("REFRESH_TOKEN,ADMIN:USER.UPDATE");
		cfg.setPublishMode(RabbitMqConfig.PublishMode.SYNC);
		cfg.setPublishBufferMaxMb(100);
	}

	@Test
	void excludedEventsAreNotPublished() throws Exception {
		RabbitMqConnectionManager connectionManager = mock(RabbitMqConnectionManager.class);
		when(connectionManager.getChannel()).thenReturn(channel);
		RabbitMqEventListenerProvider provider = new RabbitMqEventListenerProvider(new RabbitMqPublisher(connectionManager, cfg), null, session, cfg);

		provider.onEvent(event(EventType.REFRESH_TOKEN));
		provider.onEvent(event(EventType.LOGIN));
		provider.onEvent(adminEvent(OperationType.UPDATE), false);
		provider.onEvent(adminEvent(OperationType.CREATE), false);
		complete();

		verify(channel).basicPublish(eq("amq.topic"), eq("KK.EVENT.CLIENT.test.SUCCESS.my-client.LOGIN"), any(AMQP.BasicProperties.class), any(byte[].class));
		verify(channel).basicPublish(eq("amq.topic"), eq("KK.EVENT.ADMIN.test.SUCCESS.USER.CREATE"), any(AMQP.BasicProperties.class), any(byte[].class));
		verify(channel, never()).basicPublish(anyString(), eq("KK.EVENT.CLIENT.test.SUCCESS.my-client.REFRESH_TOKEN"), any(AMQP.BasicProperties.class), any(byte[].class));
		verify(channel, never()).basicPublish(anyString(), eq("KK.EVENT.ADMIN.test.SUCCESS.USER.UPDATE"), any(AMQP.BasicProperties.class), any(byte[].class));
	}

	/** Completes the request, which publishes its events. */
	private void complete() {
		ArgumentCaptor<KeycloakTransaction> afterCompletion = ArgumentCaptor.forClass(KeycloakTransaction.class);
		verify(session.getTransactionManager()).enlistAfterCompletion(afterCompletion.capture());
		afterCompletion.getValue().begin();
		afterCompletion.getValue().commit();
	}

	private static Event event(EventType type) {
		Event event = new Event();
		event.setType(type);
		event.setRealmId("realm-id");
		event.setClientId("my-client");
		return event;
	}

	private static AdminEvent adminEvent(OperationType operation) {
		AdminEvent event = new AdminEvent();
		event.setResourceTypeAsString("USER");
		event.setOperationType(operation);
		return event;
	}
}
