package com.github.aznamier.keycloak.event.provider;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;

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

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.context.Scope;

class RabbitMqEventListenerProviderTest {

	private KeycloakSession session;
	private RabbitMqConnectionManager connectionManager;
	private Channel channel;
	private RabbitMqConfig cfg;

	@BeforeEach
	void setUp() {
		session = mock(KeycloakSession.class, RETURNS_DEEP_STUBS);
		when(session.realms().getRealm("realm-id").getName()).thenReturn("test");
		when(session.getContext().getRealm().getName()).thenReturn("test");
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

	@Test
	void keycloaksEventIdIsTheMessageIdAndInTheBody() throws Exception {
		Event event = login();
		event.setId("user-event-id");
		AdminEvent adminEvent = new AdminEvent();
		adminEvent.setId("admin-event-id");
		adminEvent.setResourceTypeAsString("USER");
		adminEvent.setOperationType(OperationType.CREATE);

		RabbitMqEventListenerProvider provider = provider();
		provider.onEvent(event);
		provider.onEvent(adminEvent, false);
		complete();

		ArgumentCaptor<String> routingKeys = ArgumentCaptor.forClass(String.class);
		ArgumentCaptor<AMQP.BasicProperties> props = ArgumentCaptor.forClass(AMQP.BasicProperties.class);
		ArgumentCaptor<byte[]> bodies = ArgumentCaptor.forClass(byte[].class);
		verify(channel, times(2)).basicPublish(anyString(), routingKeys.capture(), props.capture(), bodies.capture());
		for (int i = 0; i < 2; i++) {
			// Keycloak publishes a request's admin events before its user events
			String expectedId = routingKeys.getAllValues().get(i).contains(".ADMIN.") ? "admin-event-id" : "user-event-id";
			String body = new String(bodies.getAllValues().get(i), StandardCharsets.UTF_8);
			assertEquals(expectedId, props.getAllValues().get(i).getMessageId());
			assertTrue(body.contains("\"id\" : \"" + expectedId + "\""), body);
		}
	}

	@Test
	void eventsWithoutAnIdGetARandomMessageId() throws Exception {
		String messageId = publishLogin().getMessageId();

		assertEquals(36, messageId.length(), "a UUID: " + messageId);
	}

	@Test
	void messagesCarryTheRequestsTrace() throws Exception {
		SpanContext trace = SpanContext.create("4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7", TraceFlags.getSampled(), TraceState.getDefault());

		AMQP.BasicProperties props;
		try (Scope scope = Span.wrap(trace).makeCurrent()) {
			props = publishLogin();
		}

		assertEquals("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", props.getHeaders().get("traceparent"));
		assertEquals("4bf92f3577b34da6a3ce929d0e0e4736", props.getHeaders().get("traceId"));
	}

	@Test
	void withoutATraceThereAreNoTraceHeaders() throws Exception {
		AMQP.BasicProperties props = publishLogin();

		assertFalse(props.getHeaders().containsKey("traceparent"));
		assertFalse(props.getHeaders().containsKey("traceId"));
	}

	/** Emits a LOGIN event, completes the request, and returns the properties it was published with. */
	private AMQP.BasicProperties publishLogin() throws Exception {
		provider().onEvent(login());
		complete();

		ArgumentCaptor<AMQP.BasicProperties> props = ArgumentCaptor.forClass(AMQP.BasicProperties.class);
		verify(channel).basicPublish(anyString(), anyString(), props.capture(), any(byte[].class));
		return props.getValue();
	}

	private RabbitMqEventListenerProvider provider() {
		return new RabbitMqEventListenerProvider(new RabbitMqPublisher(connectionManager, cfg), null, session, cfg);
	}

	/** Completes the request, which publishes its events. */
	private void complete() {
		ArgumentCaptor<KeycloakTransaction> afterCompletion = ArgumentCaptor.forClass(KeycloakTransaction.class);
		verify(session.getTransactionManager()).enlistAfterCompletion(afterCompletion.capture());
		afterCompletion.getValue().begin();
		afterCompletion.getValue().commit();
	}

	private static Event login() {
		Event event = new Event();
		event.setType(EventType.LOGIN);
		event.setRealmId("realm-id");
		event.setClientId("my-client");
		return event;
	}
}
