package com.github.aznamier.keycloak.event.provider;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
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
import java.util.concurrent.atomic.AtomicLong;
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
	// What the channel saw, in order: "P<n>" for a publish of event n, "C" / "N" for a confirm wait that acked / nacked
	private final List<String> log = Collections.synchronizedList(new ArrayList<>());
	private final AtomicLong nextSeqNo = new AtomicLong(1);
	private RabbitMqPublisher publisher;

	@BeforeEach
	void setUp() throws Exception {
		manager = mock(RabbitMqConnectionManager.class);
		channel = mock(Channel.class);
		when(manager.getChannel()).thenAnswer(i -> connected.get());
		when(channel.getNextPublishSeqNo()).thenAnswer(i -> nextSeqNo.get());
		doAnswer(i -> {
			log.add("P" + seqOf(i.getArgument(3)));
			nextSeqNo.incrementAndGet();
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
		publisher = new RabbitMqPublisher(manager, cfg(RabbitMqConfig.PublishMode.SYNC, 100));

		publisher.publish("KK.EVENT.TEST", props(), body(1));

		assertEquals(Arrays.asList("P1"), log);
	}

	@Test
	void syncModeDropsEventsWhileDisconnectedWithoutWaiting() throws Exception {
		publisher = new RabbitMqPublisher(manager, cfg(RabbitMqConfig.PublishMode.SYNC, 100));

		long start = System.nanoTime();
		publisher.publish("KK.EVENT.TEST", props(), body(1));

		assertTrue(System.nanoTime() - start < 50_000_000L, "publish() returned straight away");
		verify(channel, never()).basicPublish(anyString(), anyString(), any(AMQP.BasicProperties.class), any(byte[].class));
	}

	@Test
	void asyncModeBuffersWhileDisconnectedAndSendsInOrder() throws Exception {
		publisher = new RabbitMqPublisher(manager, cfg(RabbitMqConfig.PublishMode.ASYNC, 100));

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
		publisher = new RabbitMqPublisher(manager, cfg(RabbitMqConfig.PublishMode.ASYNC, 100));

		for (int n = 0; n < 10; n++) {
			publisher.publish("KK.EVENT.TEST", props(), body(n));
		}
		await(() -> log.size() == 10);

		assertEquals(sequence(0, 10), log);
	}

	@Test
	void fullBufferDropsNewEventsAndKeepsTheEarliest() throws Exception {
		publisher = new RabbitMqPublisher(manager, cfg(RabbitMqConfig.PublishMode.ASYNC, 1));
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
		publisher = new RabbitMqPublisher(manager, cfg(RabbitMqConfig.PublishMode.ASYNC, 100));
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
		publisher = new RabbitMqPublisher(manager, cfg(RabbitMqConfig.PublishMode.ASYNC, 100));
		publisher.publish("KK.EVENT.TEST", props(), body(0));

		long start = System.nanoTime();
		publisher.close();
		publisher = null;

		assertTrue(System.nanoTime() - start < 3_000_000_000L, "close() returned without RabbitMQ");
		assertTrue(log.isEmpty());
	}

	@Test
	void confirmsResendAFailedBatchOneEventAtATime() throws Exception {
		AtomicInteger waits = new AtomicInteger();
		when(channel.waitForConfirms(anyLong())).thenAnswer(i -> {
			// The first batch is acked, the second nacked (e.g. a queue that is full with overflow=reject-publish)
			boolean acked = waits.incrementAndGet() != 2;
			log.add(acked ? "C" : "N");
			return acked;
		});
		publisher = new RabbitMqPublisher(manager, cfg(RabbitMqConfig.PublishMode.CONFIRMED, 100));

		// While disconnected the worker holds event 0 as its batch; 1-4 wait in the buffer and form the next batch
		publisher.publish("KK.EVENT.TEST", props(), body(0));
		Thread.sleep(300);
		for (int n = 1; n < 5; n++) {
			publisher.publish("KK.EVENT.TEST", props(), body(n));
		}
		connected.set(channel);
		await(() -> log.stream().filter("C"::equals).count() == 5);

		// Event 0 is the first batch on its own; 1-4 are batched, nacked, then resent one at a time
		assertEquals(Arrays.asList("P0", "C", "P1", "P2", "P3", "P4", "N", "P1", "C", "P2", "C", "P3", "C", "P4", "C"), log);
	}

	@Test
	void confirmedModeBuffersWhileDisconnected() throws Exception {
		when(channel.waitForConfirms(anyLong())).thenAnswer(i -> {
			log.add("C");
			return true;
		});
		publisher = new RabbitMqPublisher(manager, cfg(RabbitMqConfig.PublishMode.CONFIRMED, 100));

		// Buffered while disconnected rather than dropped, as in sync mode
		publisher.publish("KK.EVENT.TEST", props(), body(0));
		connected.set(channel);
		await(() -> log.size() == 2);

		assertEquals(Arrays.asList("P0", "C"), log);
	}

	@Test
	void confirmsTreatABatchAsUnconfirmedWhenTheChannelWasRecovered() throws Exception {
		AtomicInteger waits = new AtomicInteger();
		when(channel.waitForConfirms(anyLong())).thenAnswer(i -> {
			if (waits.incrementAndGet() == 1) {
				// Automatic recovery replaced the channel: nothing outstanding to confirm, sequence numbers restart
				nextSeqNo.set(1);
			}
			log.add("C");
			return true;
		});
		connected.set(channel);
		publisher = new RabbitMqPublisher(manager, cfg(RabbitMqConfig.PublishMode.CONFIRMED, 100));

		publisher.publish("KK.EVENT.TEST", props(), body(0));
		await(() -> log.size() >= 4);

		assertEquals(Arrays.asList("P0", "C", "P0", "C"), log.subList(0, 4), "the batch is sent again");
	}

	@Test
	void confirmsNeverHaveTwoOrderedEventsInFlightTogether() throws Exception {
		when(channel.waitForConfirms(anyLong())).thenAnswer(i -> {
			log.add("C");
			return true;
		});
		publisher = new RabbitMqPublisher(manager, cfg(RabbitMqConfig.PublishMode.CONFIRMED, 100));

		// While disconnected the worker holds event 0 as its batch; 1-9 wait in the buffer
		publisher.publish("KK.EVENT.TEST", props(), body(0));
		Thread.sleep(300);
		List<Integer> ordered = Arrays.asList(3, 4, 8);
		for (int n = 1; n < 10; n++) {
			publisher.publish("KK.EVENT.TEST", props(), body(n), ordered.contains(n));
		}
		connected.set(channel);
		// Done once the batch holding event 9 is confirmed
		await(() -> log.contains("P9") && "C".equals(log.get(log.size() - 1)));

		// Each ordered event ends its batch, so the next one is only sent once it is confirmed
		assertEquals(Arrays.asList("P0", "C", "P1", "P2", "P3", "C", "P4", "C", "P5", "P6", "P7", "P8", "C", "P9", "C"), log);
	}

	// --- helpers ---

	static RabbitMqConfig cfg(RabbitMqConfig.PublishMode mode, int bufferMb) {
		RabbitMqConfig cfg = new RabbitMqConfig();
		cfg.setExchange("amq.topic");
		cfg.setPublishMode(mode);
		cfg.setConfirmTimeoutMs(1000L);
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
