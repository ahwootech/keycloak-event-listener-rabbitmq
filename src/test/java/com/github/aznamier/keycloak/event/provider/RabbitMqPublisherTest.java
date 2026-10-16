package com.github.aznamier.keycloak.event.provider;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;

class RabbitMqPublisherTest {

	private RabbitMqConnectionManager manager;
	private Channel channel;
	private final AtomicReference<Channel> connected = new AtomicReference<>();
	// What the channel saw, in order: "P<n>" for a publish of event n
	private final List<String> log = Collections.synchronizedList(new ArrayList<>());
	private RabbitMqPublisher publisher;

	@BeforeEach
	void setUp() throws Exception {
		manager = mock(RabbitMqConnectionManager.class);
		channel = mock(Channel.class);
		when(manager.getChannel()).thenAnswer(i -> connected.get());
		doAnswer(i -> {
			log.add("P" + seqOf(i.getArgument(3)));
			return null;
		}).when(channel).basicPublish(anyString(), anyString(), any(AMQP.BasicProperties.class), any(byte[].class));
	}

	@AfterEach
	void tearDown() {
		if (publisher != null) {
			publisher.close();
		}
	}

	@Test
	void syncModeSendsOnTheCallingThread() throws Exception {
		connected.set(channel);
		publisher = new RabbitMqPublisher(manager, cfg(false, 100));

		publisher.publish("KK.EVENT.TEST", props(), body(1));

		assertEquals(Arrays.asList("P1"), log);
	}

	@Test
	void syncModeDropsEventsWhileDisconnectedWithoutWaiting() throws Exception {
		publisher = new RabbitMqPublisher(manager, cfg(false, 100));

		long start = System.nanoTime();
		publisher.publish("KK.EVENT.TEST", props(), body(1));

		assertTrue(System.nanoTime() - start < 50_000_000L, "publish() returned straight away");
		verify(channel, never()).basicPublish(anyString(), anyString(), any(AMQP.BasicProperties.class), any(byte[].class));
	}

	@Test
	void asyncModeBuffersWhileDisconnectedAndSendsInOrder() throws Exception {
		publisher = new RabbitMqPublisher(manager, cfg(true, 100));

		for (int n = 0; n < 50; n++) {
			publisher.publish("KK.EVENT.TEST", props(), body(n));
		}
		Thread.sleep(1500);
		assertTrue(log.isEmpty(), "nothing is sent while disconnected");

		connected.set(channel);
		await(() -> log.size() == 50);

		assertEquals(sequence(0, 50), log);
	}

	@Test
	void asyncModeRetriesAFailedSendWithoutReordering() throws Exception {
		AtomicInteger calls = new AtomicInteger();
		doAnswer(i -> {
			if (calls.incrementAndGet() == 1) {
				throw new IOException("simulated failure");
			}
			log.add("P" + seqOf(i.getArgument(3)));
			return null;
		}).when(channel).basicPublish(anyString(), anyString(), any(AMQP.BasicProperties.class), any(byte[].class));
		connected.set(channel);
		publisher = new RabbitMqPublisher(manager, cfg(true, 100));

		for (int n = 0; n < 10; n++) {
			publisher.publish("KK.EVENT.TEST", props(), body(n));
		}
		await(() -> log.size() == 10);

		assertEquals(sequence(0, 10), log);
	}

	@Test
	void fullBufferDropsNewEventsAndKeepsTheEarliest() throws Exception {
		publisher = new RabbitMqPublisher(manager, cfg(true, 1));
		byte[] padding = new byte[100_000];

		// About 100 KB each, so a 1 MB buffer holds 10 of them
		for (int n = 0; n < 20; n++) {
			publisher.publish("KK.EVENT.TEST", props(), body(n, padding));
		}
		connected.set(channel);
		await(() -> log.size() >= 10);
		Thread.sleep(1500);

		assertEquals(sequence(0, 10), log);
	}

	@Test
	void shutdownSendsWhatIsStillBuffered() throws Exception {
		publisher = new RabbitMqPublisher(manager, cfg(true, 100));
		for (int n = 0; n < 20; n++) {
			publisher.publish("KK.EVENT.TEST", props(), body(n));
		}
		connected.set(channel);

		publisher.close();
		publisher = null;

		assertEquals(sequence(0, 20), log);
	}

	@Test
	void shutdownGivesUpAfterTheFlushTimeout() throws Exception {
		publisher = new RabbitMqPublisher(manager, cfg(true, 100));
		publisher.publish("KK.EVENT.TEST", props(), body(0));

		long start = System.nanoTime();
		publisher.close();
		publisher = null;

		assertTrue(System.nanoTime() - start < 3_000_000_000L, "close() returned without RabbitMQ");
		assertTrue(log.isEmpty());
	}

	// --- helpers ---

	static RabbitMqConfig cfg(boolean async, int bufferMb) {
		RabbitMqConfig cfg = new RabbitMqConfig();
		cfg.setExchange("amq.topic");
		cfg.setPublishMode(async ? RabbitMqConfig.PublishMode.ASYNC : RabbitMqConfig.PublishMode.SYNC);
		cfg.setPublishBufferMaxMb(bufferMb);
		cfg.setShutdownFlushTimeoutMs(500L);
		return cfg;
	}

	static AMQP.BasicProperties props() {
		return new AMQP.BasicProperties.Builder().contentType("application/json").build();
	}

	static byte[] body(int seq) {
		return (seq + ";").getBytes(StandardCharsets.UTF_8);
	}

	static byte[] body(int seq, byte[] padding) {
		byte[] prefix = body(seq);
		byte[] out = Arrays.copyOf(prefix, prefix.length + padding.length);
		System.arraycopy(padding, 0, out, prefix.length, padding.length);
		return out;
	}

	static int seqOf(byte[] body) {
		String s = new String(body, 0, Math.min(body.length, 12), StandardCharsets.UTF_8);
		return Integer.parseInt(s.substring(0, s.indexOf(';')));
	}

	static List<String> sequence(int from, int to) {
		List<String> out = new ArrayList<>();
		for (int n = from; n < to; n++) {
			out.add("P" + n);
		}
		return out;
	}

	static void await(BooleanSupplier condition) throws InterruptedException {
		long deadline = System.currentTimeMillis() + 15_000;
		while (!condition.getAsBoolean()) {
			if (System.currentTimeMillis() > deadline) {
				throw new AssertionError("condition not met within 15s");
			}
			Thread.sleep(20);
		}
	}
}
