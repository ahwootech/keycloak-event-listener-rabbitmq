package com.github.aznamier.keycloak.event.provider;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BooleanSupplier;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.keycloak.Config.Scope;
import org.keycloak.events.Event;
import org.keycloak.events.EventListenerProvider;
import org.keycloak.events.EventType;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakTransactionManager;
import org.keycloak.models.RealmModel;
import org.keycloak.models.RealmProvider;
import org.keycloak.services.DefaultKeycloakTransactionManager;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.ToxicDirection;

/**
 * Runs the real plugin factory against a real RabbitMQ in Docker. The plugin connects through Toxiproxy,
 * so tests can cut the link the way a failed host does: packets go nowhere and nothing is closed.
 */
@Testcontainers
@EnabledIf(DockerRequirement.CONDITION)
class RabbitMqResilienceIT {

	private static final Network network = Network.newNetwork();

	@Container
	private static final RabbitMQContainer rabbitmq = new RabbitMQContainer("rabbitmq:4.1-alpine")
			.withNetwork(network)
			.withNetworkAliases("rabbitmq");

	@Container
	private static final ToxiproxyContainer toxiproxy = new ToxiproxyContainer("ghcr.io/shopify/toxiproxy:2.12.0")
			.withNetwork(network);

	// Keycloak 26.1+ traces every commit through a TracingProvider; looked up by name, as Keycloak 26.0 has none
	private static final String TRACING_PROVIDER = "org.keycloak.tracing.TracingProvider";
	private static final Object NOOP_TRACING = newInstanceIfPresent("org.keycloak.tracing.NoopTracingProvider");

	private static final int PROXY_PORT = 8666;
	private static final String QUEUE = "kk-test";

	private Proxy proxy;
	private Connection testConnection;
	private Channel testChannel;
	private final Map<String, String> settings = new HashMap<>();
	private RabbitMqEventListenerProviderFactory factory;

	@BeforeEach
	void setUp() throws Exception {
		ToxiproxyClient client = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort());
		Proxy existing = client.getProxyOrNull("rabbitmq");
		if (existing != null) {
			existing.delete();
		}
		proxy = client.createProxy("rabbitmq", "0.0.0.0:" + PROXY_PORT, "rabbitmq:5672");

		ConnectionFactory cf = new ConnectionFactory();
		cf.setHost(rabbitmq.getHost());
		cf.setPort(rabbitmq.getAmqpPort());
		cf.setUsername(rabbitmq.getAdminUsername());
		cf.setPassword(rabbitmq.getAdminPassword());
		testConnection = cf.newConnection();
		testChannel = testConnection.createChannel();
		testChannel.queueDelete(QUEUE);
		testChannel.queueDeclare(QUEUE, true, false, false, null);
		testChannel.queueBind(QUEUE, "amq.topic", "KK.EVENT.#");

		settings.put("url", toxiproxy.getHost());
		settings.put("port", String.valueOf(toxiproxy.getMappedPort(PROXY_PORT)));
		settings.put("username", rabbitmq.getAdminUsername());
		settings.put("password", rabbitmq.getAdminPassword());
		settings.put("vhost", "/");
		// Retry and notice a dead host quickly, so recovery doesn't slow the tests down
		settings.put("network_recovery_interval_ms", "200");
		settings.put("connection_timeout_ms", "1000");
		settings.put("heartbeat_seconds", "2");
	}

	@AfterEach
	void tearDown() throws Exception {
		if (factory != null) {
			factory.close();
		}
		testConnection.close();
	}

	@Test
	void keycloakStartsWhileRabbitMqIsUnreachableAndConnectsOnceItIsBack() throws Exception {
		blackHole();
		startPlugin();

		// While RabbitMQ is unreachable, a request that emits an event isn't held up
		long start = System.currentTimeMillis();
		sendEvent("lost-while-down");
		assertTrue(System.currentTimeMillis() - start < 500, "the request waited on RabbitMQ");

		// Attempts time out on the dead host (1s each) in the background, never on a request
		RabbitMqConnectionManager manager = manager();
		Thread.sleep(3000);
		assertNull(manager.getChannel());

		restore();
		await(() -> manager.getChannel() != null);

		sendEvent("after-recovery");
		GetResponse received = awaitMessage();
		assertTrue(new String(received.getBody(), StandardCharsets.UTF_8).contains("after-recovery"));
		assertNull(testChannel.basicGet(QUEUE, true), "the event sent while down should have been dropped");
	}

	@Test
	void aHostThatDiesWithTheConnectionOpenIsNoticedWithinAFewHeartbeats() throws Exception {
		startPlugin();
		RabbitMqConnectionManager manager = manager();
		await(() -> manager.getChannel() != null);

		// Nothing is closed, so only missed heartbeats (2s here) reveal it
		blackHole();
		long start = System.currentTimeMillis();
		await(() -> manager.getChannel() == null);
		long noticedMs = System.currentTimeMillis() - start;
		assertTrue(noticedMs < 10_000, "took " + noticedMs + " ms to notice the dead host");

		// From then on, requests don't wait
		long requestStart = System.currentTimeMillis();
		sendEvent("after-noticed");
		assertTrue(System.currentTimeMillis() - requestStart < 500, "the request waited on RabbitMQ");

		restore();
		await(() -> manager.getChannel() != null);
	}

	@Test
	void withSeveralHostsItConnectsAndRecoversThroughWhicheverIsReachable() throws Exception {
		// A second route to the same RabbitMQ, so each "host" can fail on its own
		ToxiproxyClient client = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort());
		Proxy existing = client.getProxyOrNull("rabbitmq-b");
		if (existing != null) {
			existing.delete();
		}
		Proxy proxyB = client.createProxy("rabbitmq-b", "0.0.0.0:" + (PROXY_PORT + 1), "rabbitmq:5672");
		String hostA = toxiproxy.getHost() + ":" + toxiproxy.getMappedPort(PROXY_PORT);
		String hostB = toxiproxy.getHost() + ":" + toxiproxy.getMappedPort(PROXY_PORT + 1);
		settings.put("url", hostA + "," + hostB);
		try {
			// Only B is reachable at startup
			proxy.disable();
			startPlugin();
			RabbitMqConnectionManager manager = manager();
			await(() -> manager.getChannel() != null);

			// B fails like a dead host, A is back: recovery has to switch to A
			proxy.enable();
			proxyB.toxics().timeout("black-hole-up", ToxicDirection.UPSTREAM, 0);
			proxyB.toxics().timeout("black-hole-down", ToxicDirection.DOWNSTREAM, 0);
			await(() -> manager.getChannel() == null);
			await(() -> manager.getChannel() != null);

			sendEvent("after-failover");
			assertTrue(new String(awaitMessage().getBody(), StandardCharsets.UTF_8).contains("after-failover"));
		} finally {
			proxyB.delete();
		}
	}

	private void startPlugin() {
		Scope scope = mock(Scope.class);
		when(scope.get(anyString())).thenAnswer(i -> settings.get(i.<String>getArgument(0)));
		factory = new RabbitMqEventListenerProviderFactory();
		factory.init(scope);
		factory.postInit(null);
	}

	/**
	 * Emits a LOGIN event in a Keycloak transaction and commits it, the way a request does.
	 */
	private void sendEvent(String userId) {
		RealmModel realm = mock(RealmModel.class);
		when(realm.getName()).thenReturn("test");
		RealmProvider realms = mock(RealmProvider.class);
		when(realms.getRealm("realm-id")).thenReturn(realm);
		// No JTA lookup provider, as in Keycloak when JTA is off, and no-op tracing
		KeycloakSession session = mock(KeycloakSession.class);
		when(session.realms()).thenReturn(realms);
		doAnswer(i -> TRACING_PROVIDER.equals(i.<Class<?>>getArgument(0).getName()) ? NOOP_TRACING : null)
				.when(session).getProvider(any());
		KeycloakTransactionManager tm = new DefaultKeycloakTransactionManager(session);
		when(session.getTransactionManager()).thenReturn(tm);
		tm.begin();

		EventListenerProvider provider = factory.create(session);
		Event event = new Event();
		event.setType(EventType.LOGIN);
		event.setRealmId("realm-id");
		event.setClientId("my-client");
		event.setUserId(userId);
		event.setTime(System.currentTimeMillis());
		provider.onEvent(event);

		tm.commit();
	}

	private RabbitMqConnectionManager manager() throws Exception {
		Field field = RabbitMqEventListenerProviderFactory.class.getDeclaredField("connectionManager");
		field.setAccessible(true);
		return (RabbitMqConnectionManager) field.get(factory);
	}

	/** Like a failed host: the connection is accepted but no data gets through and nothing is closed. */
	private void blackHole() throws IOException {
		proxy.toxics().timeout("black-hole-up", ToxicDirection.UPSTREAM, 0);
		proxy.toxics().timeout("black-hole-down", ToxicDirection.DOWNSTREAM, 0);
	}

	private void restore() throws IOException {
		proxy.toxics().get("black-hole-up").remove();
		proxy.toxics().get("black-hole-down").remove();
	}

	private GetResponse awaitMessage() throws Exception {
		long deadline = System.currentTimeMillis() + 10_000;
		while (System.currentTimeMillis() < deadline) {
			GetResponse response = testChannel.basicGet(QUEUE, true);
			if (response != null) {
				return response;
			}
			Thread.sleep(50);
		}
		throw new AssertionError("no message arrived");
	}

	private static Object newInstanceIfPresent(String className) {
		try {
			return Class.forName(className).getDeclaredConstructor().newInstance();
		} catch (ClassNotFoundException e) {
			return null;
		} catch (ReflectiveOperationException e) {
			throw new IllegalStateException(e);
		}
	}

	private static void await(BooleanSupplier condition) throws InterruptedException {
		long deadline = System.currentTimeMillis() + 60_000;
		while (!condition.getAsBoolean()) {
			if (System.currentTimeMillis() > deadline) {
				throw new AssertionError("timed out");
			}
			Thread.sleep(50);
		}
	}
}
