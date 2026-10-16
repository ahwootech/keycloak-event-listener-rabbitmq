package com.github.aznamier.keycloak.event.provider;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.keycloak.events.Event;
import org.keycloak.events.EventType;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakTransaction;
import org.mockito.ArgumentCaptor;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;

class RabbitMqEventListenerProviderTest {

	private KeycloakSession session;
	private RabbitMqConnectionManager connectionManager;
	private Channel channel;
	private RabbitMqConfig cfg;

	@BeforeEach
	void setUp() {
		session = mock(KeycloakSession.class, RETURNS_DEEP_STUBS);
		when(session.realms().getRealm("realm-id").getName()).thenReturn("test");
		channel = mock(Channel.class);
		connectionManager = mock(RabbitMqConnectionManager.class);
		when(connectionManager.getChannel()).thenReturn(channel);
		cfg = new RabbitMqConfig();
		cfg.setExchange("amq.topic");
		cfg.setPublishMode(RabbitMqConfig.PublishMode.SYNC);
		cfg.setPublishBufferMaxMb(100);
	}

	@Test
	void messagesArePersistentWhenTurnedOn() throws Exception {
		cfg.setPersistentMessages(true);

		assertEquals(2, publishLogin().getDeliveryMode());
	}

	@Test
	void messagesAreNotPersistentByDefault() throws Exception {
		cfg.setPersistentMessages(false);

		assertNull(publishLogin().getDeliveryMode(), "RabbitMQ's default, non-persistent");
	}

	/** Emits a LOGIN event, completes the request, and returns the properties it was published with. */
	private AMQP.BasicProperties publishLogin() throws Exception {
		RabbitMqEventListenerProvider provider = new RabbitMqEventListenerProvider(new RabbitMqPublisher(connectionManager, cfg), session, cfg);
		Event event = new Event();
		event.setType(EventType.LOGIN);
		event.setRealmId("realm-id");
		event.setClientId("my-client");
		provider.onEvent(event);

		ArgumentCaptor<KeycloakTransaction> afterCompletion = ArgumentCaptor.forClass(KeycloakTransaction.class);
		verify(session.getTransactionManager()).enlistAfterCompletion(afterCompletion.capture());
		afterCompletion.getValue().begin();
		afterCompletion.getValue().commit();

		ArgumentCaptor<AMQP.BasicProperties> props = ArgumentCaptor.forClass(AMQP.BasicProperties.class);
		verify(channel).basicPublish(anyString(), anyString(), props.capture(), any(byte[].class));
		return props.getValue();
	}
}
