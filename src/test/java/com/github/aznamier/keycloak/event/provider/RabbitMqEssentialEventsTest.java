package com.github.aznamier.keycloak.event.provider;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.keycloak.events.Details;
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
 * The provider's handling of essential events: sent through the essential publisher before the request commits,
 * and the request rolled back when RabbitMQ doesn't confirm them.
 */
class RabbitMqEssentialEventsTest {

	private KeycloakSession session;
	private Channel channel;
	private RabbitMqConnectionManager connectionManager;
	private RabbitMqEssentialPublisher essentialPublisher;
	private RabbitMqConfig cfg;

	@BeforeEach
	void setUp() {
		session = mock(KeycloakSession.class, RETURNS_DEEP_STUBS);
		when(session.realms().getRealm("realm-id").getName()).thenReturn("test");
		when(session.getContext().getRealm().getName()).thenReturn("test");
		when(session.getTransactionManager().isActive()).thenReturn(true);
		channel = mock(Channel.class);
		connectionManager = mock(RabbitMqConnectionManager.class);
		when(connectionManager.getChannel()).thenReturn(channel);
		essentialPublisher = mock(RabbitMqEssentialPublisher.class);
		cfg = new RabbitMqConfig();
		cfg.setExchange("amq.topic");
		cfg.setPublishMode(RabbitMqConfig.PublishMode.SYNC);
		cfg.setPublishBufferMaxMb(100);
		cfg.setEssentialEvents("REGISTER,ADMIN:USER.DELETE");
		cfg.setEssentialEventsTimeoutMs(1000L);
	}

	@Test
	void essentialEventsAreSentPersistentWithKeycloaksEventIdBeforeTheRequestCommits() throws Exception {
		confirm();
		RabbitMqEventListenerProvider provider = provider();
		Event event = event(EventType.REGISTER, "form");
		event.setId("register-event-id");

		provider.onEvent(event);
		prepare().commit();

		ArgumentCaptor<AMQP.BasicProperties> props = ArgumentCaptor.forClass(AMQP.BasicProperties.class);
		verify(essentialPublisher).send(eq("KK.EVENT.CLIENT.test.SUCCESS.my-client.REGISTER"), props.capture(), any(byte[].class), anyLong());
		assertEquals("register-event-id", props.getValue().getMessageId());
		assertEquals(2, props.getValue().getDeliveryMode(), "essential events are always persistent");
		verify(channel, never()).basicPublish(anyString(), anyString(), any(AMQP.BasicProperties.class), any(byte[].class));
	}

	@Test
	void essentialAdminEventsAreSentBeforeTheRequestCommits() throws Exception {
		confirm();
		RabbitMqEventListenerProvider provider = provider();
		AdminEvent adminEvent = new AdminEvent();
		adminEvent.setResourceTypeAsString("USER");
		adminEvent.setOperationType(OperationType.DELETE);

		provider.onEvent(adminEvent, false);
		prepare().commit();

		verify(essentialPublisher).send(eq("KK.EVENT.ADMIN.test.SUCCESS.USER.DELETE"), any(AMQP.BasicProperties.class), any(byte[].class), anyLong());
	}

	@Test
	void anEssentialEventRabbitMqDidNotConfirmRollsTheRequestBack() {
		when(essentialPublisher.send(anyString(), any(AMQP.BasicProperties.class), any(byte[].class), anyLong()))
				.thenReturn(CompletableFuture.failedFuture(new IOException("not connected to RabbitMQ")));
		RabbitMqEventListenerProvider provider = provider();

		provider.onEvent(event(EventType.REGISTER, "form"));
		KeycloakTransaction prepare = prepare();

		// Keycloak rolls the request back when a prepare transaction throws
		IllegalStateException e = assertThrows(IllegalStateException.class, prepare::commit);
		assertTrue(e.getMessage().contains("not connected to RabbitMQ"), e.getMessage());
	}

	@Test
	void eventsThatAreNotEssentialArePublishedAsBefore() throws Exception {
		RabbitMqEventListenerProvider provider = provider();

		provider.onEvent(event(EventType.LOGIN, null));
		provider.onEvent(event(EventType.REGISTER, "broker"));
		complete();

		verify(essentialPublisher, never()).send(anyString(), any(AMQP.BasicProperties.class), any(byte[].class), anyLong());
		verify(session.getTransactionManager(), never()).enlistPrepare(any());
		verify(channel).basicPublish(eq("amq.topic"), eq("KK.EVENT.CLIENT.test.SUCCESS.my-client.LOGIN"), any(AMQP.BasicProperties.class), any(byte[].class));
		verify(channel).basicPublish(eq("amq.topic"), eq("KK.EVENT.CLIENT.test.SUCCESS.my-client.REGISTER"), any(AMQP.BasicProperties.class), any(byte[].class));
	}

	private RabbitMqEventListenerProvider provider() {
		return new RabbitMqEventListenerProvider(new RabbitMqPublisher(connectionManager, cfg), essentialPublisher, session, cfg);
	}

	private void confirm() {
		when(essentialPublisher.send(anyString(), any(AMQP.BasicProperties.class), any(byte[].class), anyLong()))
				.thenReturn(CompletableFuture.completedFuture(null));
	}

	/** The essential events transaction the provider enlisted to run before the request commits, begun. */
	private KeycloakTransaction prepare() {
		ArgumentCaptor<KeycloakTransaction> prepare = ArgumentCaptor.forClass(KeycloakTransaction.class);
		verify(session.getTransactionManager()).enlistPrepare(prepare.capture());
		prepare.getValue().begin();
		return prepare.getValue();
	}

	/** Completes the request, which publishes the normal events. */
	private void complete() {
		ArgumentCaptor<KeycloakTransaction> afterCompletion = ArgumentCaptor.forClass(KeycloakTransaction.class);
		verify(session.getTransactionManager()).enlistAfterCompletion(afterCompletion.capture());
		afterCompletion.getValue().begin();
		afterCompletion.getValue().commit();
	}

	private static Event event(EventType type, String registerMethod) {
		Event event = new Event();
		event.setType(type);
		event.setRealmId("realm-id");
		event.setClientId("my-client");
		if (registerMethod != null) {
			Map<String, String> details = new HashMap<>();
			details.put(Details.REGISTER_METHOD, registerMethod);
			event.setDetails(details);
		}
		return event;
	}
}
