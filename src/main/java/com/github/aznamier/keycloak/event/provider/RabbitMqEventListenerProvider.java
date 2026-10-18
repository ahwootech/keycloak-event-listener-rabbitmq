package com.github.aznamier.keycloak.event.provider;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.jboss.logging.Logger;
import org.keycloak.events.Event;
import org.keycloak.events.EventListenerProvider;
import org.keycloak.events.EventListenerTransaction;
import org.keycloak.events.admin.AdminEvent;
import org.keycloak.models.AbstractKeycloakTransaction;
import org.keycloak.models.KeycloakSession;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.AMQP.BasicProperties.Builder;

public class RabbitMqEventListenerProvider implements EventListenerProvider {

	private static final Logger log = Logger.getLogger(RabbitMqEventListenerProvider.class);

	private final RabbitMqConfig cfg;
	private final RabbitMqPublisher publisher;
	// Null when no essential events are configured
	private final RabbitMqEssentialPublisher essentialPublisher;

	private final KeycloakSession session;

	private final EventListenerTransaction tx = new EventListenerTransaction(this::publishAdminEvent, this::publishEvent);
	// Created on the first essential event of the request
	private EssentialEventsTransaction essentialTx;

	// Created only by RabbitMqEventListenerProviderFactory; not part of the plugin's API, so it can change in any release
	RabbitMqEventListenerProvider(RabbitMqPublisher publisher, RabbitMqEssentialPublisher essentialPublisher,
			KeycloakSession session, RabbitMqConfig cfg) {
		this.cfg = cfg;
		this.publisher = publisher;
		this.essentialPublisher = essentialPublisher;
		this.session = session;
		session.getTransactionManager().enlistAfterCompletion(tx);
	}

	@Override
	public void close() {

	}

	@Override
	public void onEvent(Event event) {
		if (cfg.isExcluded(event)) {
			return;
		}
		if (cfg.isEssential(event) && enlistEssential()) {
			Event copy = event.clone();
			essentialTx.add(() -> sendEssential(RabbitMqConfig.calculateRoutingKey(copy, session),
					EventClientNotificationMqMsg.create(copy), copy.getId(), EventClientNotificationMqMsg.class.getName()));
			return;
		}
		tx.addEvent(event.clone());
	}

	@Override
	public void onEvent(AdminEvent adminEvent, boolean includeRepresentation) {
		if (cfg.isExcluded(adminEvent)) {
			return;
		}
		if (cfg.isEssential(adminEvent) && enlistEssential()) {
			essentialTx.add(() -> sendEssential(RabbitMqConfig.calculateRoutingKey(adminEvent, session),
					EventAdminNotificationMqMsg.create(adminEvent), adminEvent.getId(), EventAdminNotificationMqMsg.class.getName()));
			return;
		}
		tx.addAdminEvent(adminEvent, includeRepresentation);
	}

	/**
	 * Makes sure the essential events transaction is enlisted to run before the request's transaction commits.
	 * Returns false if that is not possible, in which case the event is published like any other.
	 */
	private boolean enlistEssential() {
		if (essentialPublisher == null) {
			return false;
		}
		if (essentialTx == null) {
			if (!session.getTransactionManager().isActive()) {
				log.warnf("keycloak-to-rabbitmq essential event outside an active transaction, publishing it as a normal event");
				return false;
			}
			essentialTx = new EssentialEventsTransaction();
			session.getTransactionManager().enlistPrepare(essentialTx);
		}
		return true;
	}

	private PendingEssential sendEssential(String routingKey, Object msg, String eventId, String className) {
		byte[] body = RabbitMqConfig.writeAsJson(msg, true).getBytes(StandardCharsets.UTF_8);
		// The messageId is also what RabbitMQ's return of an unroutable message is matched to the event by
		BasicProperties props = getMessageProps(className, eventId).builder()
				// Always persistent, so an event RabbitMQ has confirmed also survives a broker restart
				.deliveryMode(2)
				.build();
		long deadline = System.currentTimeMillis() + cfg.getEssentialEventsTimeoutMs();
		return new PendingEssential(routingKey, essentialPublisher.send(routingKey, props, body, deadline), deadline);
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

	private interface EssentialSend {
		PendingEssential start();
	}

	private static final class PendingEssential {
		private final String routingKey;
		private final CompletableFuture<Void> result;
		private final long deadline;

		PendingEssential(String routingKey, CompletableFuture<Void> result, long deadline) {
			this.routingKey = routingKey;
			this.result = result;
			this.deadline = deadline;
		}
	}

	/**
	 * Runs before the request's transaction commits: sends the request's essential events and waits for RabbitMQ
	 * to confirm them. Throwing makes Keycloak roll the whole request back instead of committing it.
	 */
	private final class EssentialEventsTransaction extends AbstractKeycloakTransaction {
		private final List<EssentialSend> sends = new ArrayList<>();

		void add(EssentialSend send) {
			sends.add(send);
		}

		@Override
		protected void commitImpl() {
			List<PendingEssential> pending = new ArrayList<>();
			for (EssentialSend send : sends) {
				pending.add(send.start());
			}
			for (PendingEssential p : pending) {
				try {
					RabbitMqEssentialPublisher.await(p.result, p.deadline);
				} catch (Exception e) {
					for (PendingEssential other : pending) {
						other.result.cancel(false);
					}
					log.errorf("keycloak-to-rabbitmq PUBLISH FAILED: essential event %s was not sent: %s; the action is rolled back",
							p.routingKey, e.getMessage());
					throw new IllegalStateException("keycloak-to-rabbitmq essential event " + p.routingKey
							+ " was not confirmed by RabbitMQ: " + e.getMessage(), e);
				}
			}
		}

		@Override
		protected void rollbackImpl() {
			// Nothing has been sent: essential events are only sent when the request commits
		}
	}

}
