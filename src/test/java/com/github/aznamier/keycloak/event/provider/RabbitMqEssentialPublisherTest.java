package com.github.aznamier.keycloak.event.provider;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;

class RabbitMqEssentialPublisherTest {

	private RabbitMqConnectionManager manager;
	private Channel channel;
	private final AtomicLong nextSeqNo = new AtomicLong(1);
	private RabbitMqEssentialPublisher publisher;

	@BeforeEach
	void setUp() throws Exception {
		manager = mock(RabbitMqConnectionManager.class);
		channel = mock(Channel.class);
		when(manager.getEssentialChannel()).thenReturn(channel);
		when(channel.getNextPublishSeqNo()).thenAnswer(i -> nextSeqNo.get());
		doAnswer(i -> {
			nextSeqNo.incrementAndGet();
			return null;
		}).when(channel).basicPublish(anyString(), anyString(), eq(true), any(AMQP.BasicProperties.class), any(byte[].class));
		RabbitMqConfig cfg = new RabbitMqConfig();
		cfg.setExchange("amq.topic");
		publisher = new RabbitMqEssentialPublisher(manager, cfg);
	}

	@AfterEach
	void tearDown() {
		publisher.close();
	}

	@Test
	void confirmedEventsSucceedAndAreSentAsMandatory() throws Exception {
		when(channel.waitForConfirms(anyLong())).thenReturn(true);

		sendAndAwait(2000);

		verify(channel).basicPublish(eq("amq.topic"), eq("KK.EVENT.TEST"), eq(true), any(AMQP.BasicProperties.class), any(byte[].class));
	}

	@Test
	void failsStraightAwayWhenNotConnected() throws Exception {
		when(manager.getEssentialChannel()).thenReturn(null);

		long start = System.currentTimeMillis();
		IOException e = assertThrows(IOException.class, () -> sendAndAwait(5000));

		assertTrue(System.currentTimeMillis() - start < 500, "did not wait for the timeout");
		assertTrue(e.getMessage().contains("not connected"), e.getMessage());
		verify(channel, never()).basicPublish(anyString(), anyString(), eq(true), any(AMQP.BasicProperties.class), any(byte[].class));
	}

	@Test
	void unroutableEventsFail() throws Exception {
		when(channel.waitForConfirms(anyLong())).thenReturn(true);
		when(manager.consumeReturned(anyString())).thenReturn(true);

		IOException e = assertThrows(IOException.class, () -> sendAndAwait(2000));

		assertTrue(e.getMessage().contains("no queue"), e.getMessage());
	}

	@Test
	void nackedEventsFail() throws Exception {
		when(channel.waitForConfirms(anyLong())).thenReturn(false);

		IOException e = assertThrows(IOException.class, () -> sendAndAwait(2000));

		assertTrue(e.getMessage().contains("nacked"), e.getMessage());
	}

	@Test
	void confirmTimeoutsGiveTheReason() throws Exception {
		// The client's own TimeoutException has no message, and can reach the request before its own wait times out
		when(channel.waitForConfirms(anyLong())).thenThrow(new TimeoutException());
		long deadline = System.currentTimeMillis() + 2000;

		TimeoutException e = assertThrows(TimeoutException.class,
				() -> RabbitMqEssentialPublisher.await(publisher.send("KK.EVENT.TEST", props(), new byte[0], deadline), deadline));

		assertEquals("not confirmed within the essential events timeout", e.getMessage());
	}

	@Test
	void eventsConfirmedOnARecoveredChannelFail() throws Exception {
		when(channel.waitForConfirms(anyLong())).thenAnswer(i -> {
			// Automatic recovery replaced the channel: nothing outstanding to confirm, sequence numbers restart
			nextSeqNo.set(1);
			return true;
		});

		IOException e = assertThrows(IOException.class, () -> sendAndAwait(2000));

		assertTrue(e.getMessage().contains("recovered"), e.getMessage());
	}

	@Test
	void eventsThatTimedOutWhileQueuedAreNeverSent() throws Exception {
		// The first event's confirm takes 800ms, so the second waits in the queue past its 200ms deadline
		when(channel.waitForConfirms(anyLong())).thenAnswer(i -> {
			Thread.sleep(800);
			return true;
		});
		long firstDeadline = System.currentTimeMillis() + 2000;
		CompletableFuture<Void> first = publisher.send("KK.EVENT.TEST", props(), new byte[0], firstDeadline);
		long secondDeadline = System.currentTimeMillis() + 200;
		CompletableFuture<Void> second = publisher.send("KK.EVENT.TEST", props(), new byte[0], secondDeadline);

		assertThrows(TimeoutException.class, () -> RabbitMqEssentialPublisher.await(second, secondDeadline));
		RabbitMqEssentialPublisher.await(first, firstDeadline);
		Thread.sleep(300);

		verify(channel, times(1)).basicPublish(anyString(), anyString(), eq(true), any(AMQP.BasicProperties.class), any(byte[].class));
	}

	@Test
	void closeFailsEventsThatAreStillWaiting() throws Exception {
		when(channel.waitForConfirms(anyLong())).thenAnswer(i -> {
			Thread.sleep(10_000);
			return true;
		});
		long deadline = System.currentTimeMillis() + 10_000;
		CompletableFuture<Void> inFlight = publisher.send("KK.EVENT.TEST", props(), new byte[0], deadline);
		CompletableFuture<Void> queued = publisher.send("KK.EVENT.TEST", props(), new byte[0], deadline);
		Thread.sleep(200);

		long start = System.currentTimeMillis();
		publisher.close();

		assertThrows(IOException.class, () -> RabbitMqEssentialPublisher.await(inFlight, deadline));
		assertThrows(IOException.class, () -> RabbitMqEssentialPublisher.await(queued, deadline));
		assertTrue(System.currentTimeMillis() - start < 1000, "both failed promptly");
	}

	private void sendAndAwait(long timeoutMs) throws Exception {
		long deadline = System.currentTimeMillis() + timeoutMs;
		RabbitMqEssentialPublisher.await(publisher.send("KK.EVENT.TEST", props(), new byte[0], deadline), deadline);
	}

	private static AMQP.BasicProperties props() {
		return new AMQP.BasicProperties.Builder().messageId(UUID.randomUUID().toString()).deliveryMode(2).build();
	}
}
