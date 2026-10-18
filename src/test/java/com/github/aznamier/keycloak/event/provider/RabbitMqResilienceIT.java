package com.github.aznamier.keycloak.event.provider;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;
import org.junit.jupiter.api.condition.EnabledIf;
import org.keycloak.Config.Scope;
import org.keycloak.events.Event;
import org.keycloak.events.EventListenerProvider;
import org.keycloak.events.EventType;
import org.keycloak.models.AbstractKeycloakTransaction;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakTransaction;
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
// On a separate thread, so a request blocked on RabbitMQ fails the test instead of hanging the build
@Timeout(value = 3, unit = TimeUnit.MINUTES, threadMode = ThreadMode.SEPARATE_THREAD)
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
	private static final Pattern USER_ID = Pattern.compile("\"userId\"\\s*:\\s*\"([^\"]+)\"");

	private Proxy proxy;
	private Connection testConnection;
	private Channel testChannel;
	private final Map<String, String> settings = new HashMap<>();
	private RabbitMqEventListenerProviderFactory factory;
	// The connection manager's log lines
	private final List<String> logLines = Collections.synchronizedList(new ArrayList<>());
	private final Logger managerLogger = Logger.getLogger(RabbitMqConnectionManager.class.getName());
	private final Handler logHandler = new Handler() {
		private final SimpleFormatter formatter = new SimpleFormatter();

		@Override
		public void publish(LogRecord record) {
			logLines.add(formatter.formatMessage(record));
		}

		@Override
		public void flush() {
		}

		@Override
		public void close() {
		}
	};

	@BeforeEach
	void setUp() throws Exception {
		managerLogger.addHandler(logHandler);
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

		settings.put("hosts", toxiproxy.getHost());
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
		managerLogger.removeHandler(logHandler);
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
		settings.put("hosts", hostA + "," + hostB);
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

	@Test
	void backgroundPublishingKeepsEventsInOrderThroughAnOutage() throws Exception {
		settings.put("publish_mode", "async");
		startPlugin();
		RabbitMqConnectionManager manager = manager();
		await(() -> manager.getChannel() != null);
		warmUp();

		// Events sent into the dead connection before the plugin notices are lost.
		// Events sent once it knows it is disconnected must be kept and delivered after recovery.
		List<Integer> mustArrive = new ArrayList<>();
		long slowestMs = 0;
		int seq = 0;
		int keptWhileDown = 0;

		for (int i = 0; i < 50; i++, seq++) {
			slowestMs = Math.max(slowestMs, timedSend(seq));
		}
		blackHole();
		long deadline = System.currentTimeMillis() + 20_000;
		while (keptWhileDown < 100) {
			assertTrue(System.currentTimeMillis() < deadline, "the plugin never noticed the outage");
			boolean disconnected = manager.getChannel() == null;
			slowestMs = Math.max(slowestMs, timedSend(seq));
			if (disconnected) {
				mustArrive.add(seq);
				keptWhileDown++;
			}
			seq++;
			Thread.sleep(20);
		}
		restore();
		await(() -> manager.getChannel() != null);
		for (int i = 0; i < 50; i++, seq++) {
			slowestMs = Math.max(slowestMs, timedSend(seq));
			mustArrive.add(seq);
		}
		assertTrue(slowestMs < 200, "a request waited " + slowestMs + " ms on RabbitMQ");

		List<Integer> received = receiveAll(seq);
		for (int i = 1; i < received.size(); i++) {
			assertTrue(received.get(i) > received.get(i - 1), "out of order or duplicated: " + received);
		}
		List<Integer> missing = new ArrayList<>(mustArrive);
		missing.removeAll(received);
		assertTrue(missing.isEmpty(), "events sent while disconnected or after recovery were lost: " + missing);
	}

	@Test
	void publisherConfirmsLoseNothingWhenTheConnectionIsCutMidStream() throws Exception {
		settings.put("publish_mode", "confirmed");
		settings.put("confirm_timeout_ms", "1000");
		startPlugin();
		RabbitMqConnectionManager manager = manager();
		await(() -> manager.getChannel() != null);
		warmUp();

		long slowestMs = 0;
		int seq = 0;
		int sentBeforeNoticed = 0;
		int sentWhileDown = 0;

		for (int i = 0; i < 50; i++, seq++) {
			slowestMs = Math.max(slowestMs, timedSend(seq));
		}
		blackHole();
		long deadline = System.currentTimeMillis() + 20_000;
		while (sentWhileDown < 100) {
			assertTrue(System.currentTimeMillis() < deadline, "the plugin never noticed the outage");
			if (manager.getChannel() == null) {
				sentWhileDown++;
			} else {
				sentBeforeNoticed++;
			}
			slowestMs = Math.max(slowestMs, timedSend(seq));
			seq++;
			Thread.sleep(20);
		}
		restore();
		await(() -> manager.getChannel() != null);
		for (int i = 0; i < 50; i++, seq++) {
			slowestMs = Math.max(slowestMs, timedSend(seq));
		}
		assertTrue(sentBeforeNoticed > 0, "no events went into the dead connection, so confirms weren't tested");
		assertTrue(slowestMs < 200, "a request waited " + slowestMs + " ms on RabbitMQ");

		// An event whose confirm was lost is sent again, so it can arrive twice; none may be lost, and the
		// events must first arrive in order
		List<Integer> firstArrivals = new ArrayList<>();
		for (int received : receiveAll(seq)) {
			if (!firstArrivals.contains(received)) {
				firstArrivals.add(received);
			}
		}
		List<Integer> expected = new ArrayList<>();
		for (int i = 0; i < seq; i++) {
			expected.add(i);
		}
		assertEquals(expected, firstArrivals, "events were lost or arrived out of order");
	}

	@Test
	void orderedEventsKeepTheirOrderWhenAFullQueueRejectsPartOfABatch() throws Exception {
		// A full queue with reject-publish nacks new messages. Only publisher confirms can see that;
		// without them RabbitMQ drops the messages and the plugin can't know.
		testChannel.queueDelete(QUEUE);
		Map<String, Object> args = new HashMap<>();
		args.put("x-max-length", 20);
		args.put("x-overflow", "reject-publish");
		testChannel.queueDeclare(QUEUE, true, false, false, args);
		testChannel.queueBind(QUEUE, "amq.topic", "KK.EVENT.#");
		for (int i = 0; i < 20; i++) {
			testChannel.basicPublish("", QUEUE, null, "filler".getBytes(StandardCharsets.UTF_8));
		}

		// All events wait in the buffer while RabbitMQ is unreachable, so they are sent in large batches
		proxy.disable();
		settings.put("publish_mode", "confirmed");
		settings.put("ordered_events", "UPDATE_EMAIL");
		startPlugin();

		long slowestMs = 0;
		int count = 100;
		List<Integer> ordered = new ArrayList<>();
		for (int seq = 0; seq < count; seq++) {
			// Every other event, so events that get in ahead of earlier ones are likely to include ordered ones
			boolean isOrdered = seq % 2 == 1;
			if (isOrdered) {
				ordered.add(seq);
			}
			slowestMs = Math.max(slowestMs, timedSend(seq, isOrdered ? EventType.UPDATE_EMAIL : EventType.LOGIN));
		}
		assertTrue(slowestMs < 200, "a request waited " + slowestMs + " ms on RabbitMQ");

		// A slow link makes a batch take about a second to send, and the full queue is drained steadily meanwhile,
		// so RabbitMQ accepts part of a batch and rejects the rest. Other events can then get in ahead of
		// earlier ones; ordered events never do.
		proxy.toxics().bandwidth("slow", ToxicDirection.UPSTREAM, 50);
		proxy.enable();
		List<Integer> firstArrivals = new ArrayList<>();
		for (int received : receiveAll(count, 20)) {
			if (!firstArrivals.contains(received)) {
				firstArrivals.add(received);
			}
		}
		List<Integer> missing = new ArrayList<>();
		for (int seq = 0; seq < count; seq++) {
			if (!firstArrivals.contains(seq)) {
				missing.add(seq);
			}
		}
		assertTrue(missing.isEmpty(), "events were lost: " + missing);
		List<Integer> orderedArrivals = new ArrayList<>(firstArrivals);
		orderedArrivals.retainAll(ordered);
		assertEquals(ordered, orderedArrivals, "ordered events arrived out of order: " + firstArrivals);
	}

	@Test
	void backgroundPublishingDoesNotMakeRequestsWaitWhileRabbitMqBlocksPublishers() throws Exception {
		settings.put("publish_mode", "async");
		startPlugin();
		RabbitMqConnectionManager manager = manager();
		await(() -> manager.getChannel() != null);
		warmUp();
		String watermark = memoryWatermark();

		// A memory alarm: RabbitMQ stays connected but stops reading from publishers, so once the network
		// buffers are full, sending blocks. 2000 events of about 10 KB are far more than those buffers hold.
		// Sent on another thread, so a request that waits fails the test instead of hanging it
		int count = 2000;
		AtomicLong slowestMs = new AtomicLong();
		ExecutorService requests = Executors.newSingleThreadExecutor();
		try {
			rabbitmqctl("set_vm_memory_high_watermark", "absolute", "1MB");
			await(() -> alarmsRaised().contains("Memory alarm"));
			Future<?> sent = requests.submit(() -> {
				for (int seq = 0; seq < count; seq++) {
					long start = System.currentTimeMillis();
					sendEvent(String.valueOf(seq), 10_000);
					slowestMs.accumulateAndGet(System.currentTimeMillis() - start, Math::max);
				}
			});
			try {
				sent.get(30, TimeUnit.SECONDS);
			} catch (TimeoutException e) {
				throw new AssertionError("requests were still waiting on RabbitMQ after 30s");
			}
		} finally {
			rabbitmqctl("set_vm_memory_high_watermark", watermark);
			requests.shutdownNow();
		}
		assertTrue(slowestMs.get() < 200, "a request waited " + slowestMs.get() + " ms on RabbitMQ");

		// Sent once the alarm clears
		List<Integer> expected = new ArrayList<>();
		for (int seq = 0; seq < count; seq++) {
			expected.add(seq);
		}
		assertEquals(expected, receiveAll(count));
	}

	@Test
	void connectionLossFailedReconnectsAndRecoveryAreLogged() throws Exception {
		startPlugin();
		RabbitMqConnectionManager manager = manager();
		await(() -> manager.getChannel() != null);
		// Report the outage every half second instead of every minute
		manager.stillDownReportIntervalMs = 500;

		// A dead host: nothing is published, so only the connection itself shows the outage (missed heartbeats)
		blackHole();
		awaitLog("keycloak-to-rabbitmq CONNECTION PROBLEM: connection to RabbitMQ at " + address()
				+ " lost, reconnecting every 200 ms: com.rabbitmq.client.MissedHeartbeatException");
		awaitLog("keycloak-to-rabbitmq CONNECTION PROBLEM: still not connected to RabbitMQ at " + address() + " after ");

		restore();
		awaitLog("keycloak-to-rabbitmq reconnected to RabbitMQ at " + address() + " after ");
	}

	@Test
	void rabbitMqBlockingPublishersIsLogged() throws Exception {
		startPlugin();
		RabbitMqConnectionManager manager = manager();
		await(() -> manager.getChannel() != null);
		String watermark = memoryWatermark();

		// A memory alarm: RabbitMQ stays connected but stops accepting messages from publishers.
		// It only tells a connection it is blocked once that connection publishes during the alarm.
		try {
			rabbitmqctl("set_vm_memory_high_watermark", "absolute", "1MB");
			await(() -> alarmsRaised().contains("Memory alarm"));
			sendEvent("during-alarm");
			awaitLog("keycloak-to-rabbitmq CONNECTION PROBLEM: RabbitMQ at " + address() + " is blocking publishers: ");
		} finally {
			rabbitmqctl("set_vm_memory_high_watermark", watermark);
		}
		awaitLog("keycloak-to-rabbitmq RabbitMQ at " + address() + " unblocked publishers after ");
	}

	@Test
	void confirmedEssentialEventsLetTheRequestCommit() throws Exception {
		startEssentialPlugin();

		Outcome outcome = sendEssentialEvent("confirmed");

		outcome.assertCommitted();
		GetResponse received = awaitMessage();
		assertTrue(new String(received.getBody(), StandardCharsets.UTF_8).contains("REGISTER"));
		assertEquals(2, received.getProps().getDeliveryMode(), "essential events are always persistent");
	}

	@Test
	void essentialEventsWithNoQueueToReceiveThemRollTheRequestBack() throws Exception {
		testChannel.queueUnbind(QUEUE, "amq.topic", "KK.EVENT.#");
		startEssentialPlugin();

		sendEssentialEvent("unroutable").assertRolledBackWithin(1000, "returned as unroutable");
	}

	@Test
	void essentialEventsRejectedByAFullQueueRollTheRequestBack() throws Exception {
		testChannel.queueDelete(QUEUE);
		Map<String, Object> args = new HashMap<>();
		args.put("x-max-length", 1);
		args.put("x-overflow", "reject-publish");
		testChannel.queueDeclare(QUEUE, true, false, false, args);
		testChannel.queueBind(QUEUE, "amq.topic", "KK.EVENT.#");
		testChannel.basicPublish("", QUEUE, null, "filler".getBytes(StandardCharsets.UTF_8));
		startEssentialPlugin();

		sendEssentialEvent("rejected").assertRolledBackWithin(1000, "rejected (nacked)");
		assertEquals(1, testChannel.queueDeclarePassive(QUEUE).getMessageCount(), "only the filler is in the queue");
	}

	@Test
	void essentialEventsNotConfirmedInTimeRollTheRequestBack() throws Exception {
		startEssentialPlugin();
		// RabbitMQ stops responding, and the plugin hasn't noticed yet (that takes two missed heartbeats)
		blackHole();

		Outcome outcome = sendEssentialEvent("frozen");

		outcome.assertRolledBackWithin(3000, "not confirmed within the essential events timeout");
		assertTrue(outcome.elapsedMs >= 900, "the request should wait for the 1s timeout, took " + outcome.elapsedMs + " ms");
	}

	@Test
	void essentialEventsWhileDisconnectedRollTheRequestBackStraightAway() throws Exception {
		RabbitMqConnectionManager manager = startEssentialPlugin();
		proxy.disable();
		await(() -> manager.getEssentialChannel() == null);

		sendEssentialEvent("disconnected").assertRolledBackWithin(200, "not connected to RabbitMQ");
	}

	@Test
	void excludedEventsAreNeverPublished() throws Exception {
		settings.put("excluded_events", "REFRESH_TOKEN");
		startPlugin();
		RabbitMqConnectionManager manager = manager();
		await(() -> manager.getChannel() != null);

		sendEvent("refresh", EventType.REFRESH_TOKEN, null);
		sendEvent("login", EventType.LOGIN, null);

		assertTrue(new String(awaitMessage().getBody(), StandardCharsets.UTF_8).contains("\"userId\" : \"login\""));
		Thread.sleep(1000);
		assertNull(testChannel.basicGet(QUEUE, true), "the excluded event was published");
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
		sendEvent(userId, 0);
	}

	/** Like {@link #sendEvent(String)}, with an event detail of paddingChars characters, to make the event bigger. */
	private void sendEvent(String userId, int paddingChars) {
		sendEvent(userId, EventType.LOGIN, null, paddingChars);
	}

	/** Starts the plugin with REGISTER as an essential event and a 1s timeout, and waits until it is connected. */
	private RabbitMqConnectionManager startEssentialPlugin() throws Exception {
		settings.put("essential_events", "REGISTER");
		settings.put("essential_events_timeout_ms", "1000");
		startPlugin();
		RabbitMqConnectionManager manager = manager();
		await(() -> manager.getEssentialChannel() != null);
		return manager;
	}

	/**
	 * Emits a REGISTER in a request that also changes the database, represented by a transaction that records
	 * whether Keycloak committed or rolled it back.
	 */
	private Outcome sendEssentialEvent(String userId) {
		RecordingTransaction database = new RecordingTransaction();
		long start = System.currentTimeMillis();
		RuntimeException error = null;
		try {
			sendEvent(userId, EventType.REGISTER, database);
		} catch (RuntimeException e) {
			error = e;
		}
		return new Outcome(database, System.currentTimeMillis() - start, error);
	}

	private void sendEvent(String userId, EventType type, KeycloakTransaction database) {
		sendEvent(userId, type, database, 0);
	}

	private void sendEvent(String userId, EventType type, KeycloakTransaction database, int paddingChars) {
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
		if (database != null) {
			tm.enlist(database);
		}

		EventListenerProvider provider = factory.create(session);
		Event event = new Event();
		event.setType(type);
		event.setRealmId("realm-id");
		event.setClientId("my-client");
		event.setUserId(userId);
		event.setTime(System.currentTimeMillis());
		if (paddingChars > 0) {
			event.setDetails(Collections.singletonMap("padding", String.join("", Collections.nCopies(paddingChars, "x"))));
		}
		provider.onEvent(event);

		tm.commit();
	}

	/**
	 * Sends one event and waits for it, so the first timed request doesn't include class loading and JSON setup
	 * (over 100 ms, more on a slow CI runner).
	 */
	private void warmUp() throws Exception {
		sendEvent("warm-up");
		awaitMessage();
	}

	private long timedSend(int seq) {
		return timedSend(seq, EventType.LOGIN);
	}

	private long timedSend(int seq, EventType type) {
		long start = System.currentTimeMillis();
		sendEvent(String.valueOf(seq), type, null);
		return System.currentTimeMillis() - start;
	}

	/**
	 * Returns the sequence numbers (user IDs) of the events received, in the order they arrive, until all of
	 * 0 to count - 1 have arrived, or until none arrive for a while (10s, or 2s once the last one has arrived).
	 */
	private List<Integer> receiveAll(int count) throws Exception {
		return receiveAll(count, 0);
	}

	/** Like {@link #receiveAll(int)}, waiting delayMs after each message, to drain the queue slowly. */
	private List<Integer> receiveAll(int count, long delayMs) throws Exception {
		List<Integer> received = new ArrayList<>();
		Set<Integer> seen = new HashSet<>();
		while (seen.size() < count) {
			GetResponse response = pollMessage(seen.contains(count - 1) ? 2_000 : 10_000);
			if (response == null) {
				break;
			}
			Matcher m = USER_ID.matcher(new String(response.getBody(), StandardCharsets.UTF_8));
			if (m.find()) {
				int seq = Integer.parseInt(m.group(1));
				seen.add(seq);
				received.add(seq);
			}
			Thread.sleep(delayMs);
		}
		return received;
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
		GetResponse response = pollMessage(10_000);
		if (response == null) {
			throw new AssertionError("no message arrived");
		}
		return response;
	}

	/** Returns the next message, or null if none arrives within the timeout. */
	private GetResponse pollMessage(long timeoutMs) throws Exception {
		long deadline = System.currentTimeMillis() + timeoutMs;
		while (System.currentTimeMillis() < deadline) {
			GetResponse response = testChannel.basicGet(QUEUE, true);
			if (response != null) {
				return response;
			}
			Thread.sleep(50);
		}
		return null;
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

	private String address() {
		return toxiproxy.getHost() + ":" + toxiproxy.getMappedPort(PROXY_PORT);
	}

	/** Waits for a log line from the connection manager that starts with the given text. */
	private void awaitLog(String start) throws InterruptedException {
		long deadline = System.currentTimeMillis() + 30_000;
		while (true) {
			synchronized (logLines) {
				if (logLines.stream().anyMatch(l -> l.startsWith(start))) {
					return;
				}
			}
			if (System.currentTimeMillis() > deadline) {
				throw new AssertionError("no log line starting with:\n  " + start + "\nlogged:\n  " + String.join("\n  ", logLines));
			}
			Thread.sleep(50);
		}
	}

	private static void rabbitmqctl(String... args) throws Exception {
		String[] command = new String[args.length + 2];
		command[0] = "rabbitmqctl";
		command[1] = "-q";
		System.arraycopy(args, 0, command, 2, args.length);
		rabbitmqCli(command);
	}

	/**
	 * Runs a RabbitMQ command-line tool in the container. Under a memory alarm, rabbitmqctl sometimes never returns,
	 * which would hang the build, so each attempt is killed after 20s and retried, up to 3 times. Every command used
	 * here only reads, or sets a value that is the same when set again, so retrying is safe.
	 */
	private static org.testcontainers.containers.Container.ExecResult rabbitmqCli(String... command) throws Exception {
		String[] limited = new String[command.length + 4];
		limited[0] = "timeout";
		limited[1] = "-s";
		limited[2] = "KILL";
		limited[3] = "20";
		System.arraycopy(command, 0, limited, 4, command.length);
		org.testcontainers.containers.Container.ExecResult result = null;
		for (int attempt = 1; attempt <= 3; attempt++) {
			result = rabbitmq.execInContainer(limited);
			if (result.getExitCode() == 0) {
				return result;
			}
		}
		throw new AssertionError(String.join(" ", command) + " failed 3 times, last with exit code " + result.getExitCode()
				+ " (137 means it was killed after 20s): " + result.getStderr());
	}

	private static String alarmsRaised() {
		try {
			return rabbitmqCli("rabbitmq-diagnostics", "-q", "alarms").getStdout();
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}

	/** RabbitMQ's relative memory high watermark, e.g. "0.6", so a test can put it back. */
	private static String memoryWatermark() throws Exception {
		String status = rabbitmqCli("rabbitmq-diagnostics", "-q", "status").getStdout();
		Matcher m = Pattern.compile("Memory high watermark setting: ([0-9.]+) of available memory").matcher(status);
		assertTrue(m.find(), status);
		return m.group(1);
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

	private static final class RecordingTransaction extends AbstractKeycloakTransaction {
		volatile boolean committed;
		volatile boolean rolledBack;

		@Override
		protected void commitImpl() {
			committed = true;
		}

		@Override
		protected void rollbackImpl() {
			rolledBack = true;
		}
	}

	private static final class Outcome {
		final RecordingTransaction database;
		final long elapsedMs;
		final RuntimeException error;

		Outcome(RecordingTransaction database, long elapsedMs, RuntimeException error) {
			this.database = database;
			this.elapsedMs = elapsedMs;
			this.error = error;
		}

		void assertCommitted() {
			assertNull(error, "the request failed");
			assertTrue(database.committed && !database.rolledBack, "the database change should be committed");
		}

		void assertRolledBackWithin(long maxMs, String reason) {
			assertNotNull(error, "the request should fail");
			assertTrue(error.getMessage().contains(reason), "expected the reason \"" + reason + "\", got: " + error.getMessage());
			assertTrue(database.rolledBack && !database.committed, "the database change should be rolled back");
			assertTrue(elapsedMs < maxMs, "the request took " + elapsedMs + " ms, expected under " + maxMs);
		}
	}
}
