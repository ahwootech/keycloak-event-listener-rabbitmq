package com.github.aznamier.keycloak.event.provider;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.keycloak.events.Event;
import org.keycloak.events.EventListenerProvider;
import org.keycloak.events.EventListenerTransaction;
import org.keycloak.events.admin.AdminEvent;
import org.keycloak.models.KeycloakSession;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.AMQP.BasicProperties.Builder;

public class RabbitMqEventListenerProvider implements EventListenerProvider {

	private final RabbitMqConfig cfg;
	private final RabbitMqPublisher publisher;

	private final KeycloakSession session;

	private final EventListenerTransaction tx = new EventListenerTransaction(this::publishAdminEvent, this::publishEvent);

	// Created only by RabbitMqEventListenerProviderFactory; not part of the plugin's API, so it can change in any release
	RabbitMqEventListenerProvider(RabbitMqPublisher publisher, KeycloakSession session, RabbitMqConfig cfg) {
		this.cfg = cfg;
		this.publisher = publisher;
		this.session = session;
		session.getTransactionManager().enlistAfterCompletion(tx);
	}

	@Override
	public void close() {

	}

	@Override
	public void onEvent(Event event) {
		tx.addEvent(event.clone());
	}

	@Override
	public void onEvent(AdminEvent adminEvent, boolean includeRepresentation) {
		tx.addAdminEvent(adminEvent, includeRepresentation);
	}
	
	private void publishEvent(Event event) {
		EventClientNotificationMqMsg msg = EventClientNotificationMqMsg.create(event);
		String routingKey = RabbitMqConfig.calculateRoutingKey(event, session);
		String messageString = RabbitMqConfig.writeAsJson(msg, true);
		
		BasicProperties msgProps = this.getMessageProps(EventClientNotificationMqMsg.class.getName(), event.getId());
		this.publishNotification(messageString, msgProps, routingKey);
	}
	
	private void publishAdminEvent(AdminEvent adminEvent, boolean includeRepresentation) {
		EventAdminNotificationMqMsg msg = EventAdminNotificationMqMsg.create(adminEvent);
		String routingKey = RabbitMqConfig.calculateRoutingKey(adminEvent, session);
		String messageString = RabbitMqConfig.writeAsJson(msg, true);

		BasicProperties msgProps = this.getMessageProps(EventAdminNotificationMqMsg.class.getName(), adminEvent.getId());
		this.publishNotification(messageString,msgProps, routingKey);
	}
	
	private BasicProperties getMessageProps(String className, String eventId) {
		
		Map<String,Object> headers = new HashMap<>();
		headers.put("__TypeId__", className);
		// The trace of the request that produced the event, if Keycloak tracing is enabled; read here, on the request
		// thread, so buffered and resent events keep it
		TraceHeaders.addTo(headers);
		
		Builder propsBuilder = new AMQP.BasicProperties.Builder()
				.appId("Keycloak")
				.headers(headers)
				.contentType("application/json")
				.contentEncoding("UTF-8")
				// Keycloak's own event ID (also in the body as "id"), so consumers can detect duplicates and find the
				// event in Keycloak; a random one if Keycloak didn't set it
				.messageId(eventId != null ? eventId : UUID.randomUUID().toString());
		if (Boolean.TRUE.equals(cfg.getPersistentMessages())) {
			// Persistent delivery mode: RabbitMQ writes the message to disk, so it survives a broker restart. This is
			// set per message by the publisher, not by the queue: a durable classic queue keeps itself, but drops
			// transient messages on restart. Quorum queues and streams persist every message regardless.
			propsBuilder.deliveryMode(2);
		}
		return propsBuilder.build();
	}

	private void publishNotification(String messageString, BasicProperties props, String routingKey) {
		publisher.publish(routingKey, props, messageString.getBytes(StandardCharsets.UTF_8));
	}

}