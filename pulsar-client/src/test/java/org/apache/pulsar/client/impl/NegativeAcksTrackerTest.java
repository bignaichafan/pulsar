/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.client.impl;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.RedeliveryBackoff;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.awaitility.Awaitility;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

public class NegativeAcksTrackerTest {

    private static final long LONG_BACKOFF_MS = TimeUnit.HOURS.toMillis(1);
    private static final long SHORT_BACKOFF_MS = TimeUnit.SECONDS.toMillis(1);

    // redeliveryCount 1 -> 1 hour, otherwise -> 1 second
    private static final RedeliveryBackoff BACKOFF =
            redeliveryCount -> redeliveryCount == 1 ? LONG_BACKOFF_MS : SHORT_BACKOFF_MS;

    private static Message<?> mockMessage(MessageId messageId, int redeliveryCount) {
        Message<?> message = mock(Message.class);
        when(message.getMessageId()).thenReturn(messageId);
        when(message.getRedeliveryCount()).thenReturn(redeliveryCount);
        return message;
    }

    private static ConsumerConfigurationData<?> confWithBackoff() {
        ConsumerConfigurationData<?> conf = new ConsumerConfigurationData<>();
        conf.setNegativeAckRedeliveryBackoff(BACKOFF);
        return conf;
    }

    /**
     * A message nacked with a short backoff after one nacked with a long backoff must get a timer that fires
     * after the short backoff, not wait for the timer scheduled for the long one.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testShorterBackoffAfterLongerBackoffSchedulesEarlierTimer() {
        Timer timer = mock(Timer.class);
        when(timer.newTimeout(any(), anyLong(), any())).thenReturn(mock(Timeout.class));
        PulsarClientImpl client = mock(PulsarClientImpl.class);
        when(client.timer()).thenReturn(timer);
        ConsumerBase<byte[]> consumer = mock(ConsumerBase.class);
        when(consumer.getClient()).thenReturn(client);

        @Cleanup
        NegativeAcksTracker tracker = new NegativeAcksTracker(consumer, confWithBackoff());

        tracker.add(mockMessage(new MessageIdImpl(1L, 1L, -1), 1));
        tracker.add(mockMessage(new MessageIdImpl(1L, 2L, -1), 0));
        assertEquals(tracker.getNackedMessagesCount(), 2);

        ArgumentCaptor<Long> delays = ArgumentCaptor.forClass(Long.class);
        verify(timer, atLeastOnce()).newTimeout(any(), delays.capture(), eq(TimeUnit.MILLISECONDS));
        long earliestScheduledDelayMs = delays.getAllValues().stream().mapToLong(Long::longValue).min().getAsLong();
        assertTrue(earliestScheduledDelayMs <= SHORT_BACKOFF_MS,
                "expected a redelivery timer within " + SHORT_BACKOFF_MS + " ms, but scheduled delays were "
                        + delays.getAllValues());
    }

    /**
     * End-to-end with a real timer: the short-backoff message must be redelivered after roughly its own backoff,
     * on its own, while the long-backoff message is still pending.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testShorterBackoffAfterLongerBackoffIsRedeliveredOnTime() {
        @Cleanup("stop")
        Timer timer = new HashedWheelTimer(new DefaultThreadFactory("pulsar-timer", true), 1, TimeUnit.MILLISECONDS);
        PulsarClientImpl client = mock(PulsarClientImpl.class);
        when(client.timer()).thenReturn(timer);
        List<Set<MessageId>> redeliveries = new CopyOnWriteArrayList<>();
        ConsumerBase<byte[]> consumer = mock(ConsumerBase.class);
        when(consumer.getClient()).thenReturn(client);
        doAnswer(invocation -> {
            redeliveries.add(Set.copyOf((Set<MessageId>) invocation.getArgument(0)));
            return null;
        }).when(consumer).redeliverUnacknowledgedMessages(any(Set.class));

        @Cleanup
        NegativeAcksTracker tracker = new NegativeAcksTracker(consumer, confWithBackoff());

        tracker.add(mockMessage(new MessageIdImpl(1L, 1L, -1), 1));
        tracker.add(mockMessage(new MessageIdImpl(1L, 2L, -1), 0));

        Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertEquals(redeliveries.size(), 1));
        assertEquals(redeliveries.get(0), Set.of(new MessageIdImpl(1L, 2L, -2)));
        assertEquals(tracker.getNackedMessagesCount(), 1);
    }
}
