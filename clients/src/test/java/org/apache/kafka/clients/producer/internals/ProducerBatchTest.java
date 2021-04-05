/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Deque;
import java.util.concurrent.ExecutionException;

import static org.apache.kafka.common.record.RecordBatch.MAGIC_VALUE_V0;
import static org.apache.kafka.common.record.RecordBatch.MAGIC_VALUE_V1;
import static org.apache.kafka.common.record.RecordBatch.MAGIC_VALUE_V2;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class ProducerBatchTest {

    private final long now = 1488748346917L;

    private final MemoryRecordsBuilder memoryRecordsBuilder = MemoryRecords.builder(ByteBuffer.allocate(128),
            CompressionType.NONE, TimestampType.CREATE_TIME, 128);

    @Test
    public void testChecksumNullForMagicV2() {
        ProducerBatch batch = new ProducerBatch(new TopicPartition("topic", 1), memoryRecordsBuilder, now);
        FutureRecordMetadata future = batch.tryAppend(now, null, new byte[10], Record.EMPTY_HEADERS, null, now);
        assertNotNull(future);
    }

    @Test
    public void testBatchAbort() throws Exception {
        ProducerBatch batch = new ProducerBatch(new TopicPartition("topic", 1), memoryRecordsBuilder, now);
        MockCallback callback = new MockCallback();
        FutureRecordMetadata future = batch.tryAppend(now, null, new byte[10], Record.EMPTY_HEADERS, callback, now);

        KafkaException exception = new KafkaException();
        batch.abort(exception);
        assertTrue(future.isDone());
        assertEquals(1, callback.invocations);
        assertEquals(exception, callback.exception);
        assertNull(callback.metadata);

        // subsequent completion should be ignored
        assertFalse(batch.done(500L, 2342342341L, null));
        assertFalse(batch.done(-1, -1, new KafkaException()));
        assertEquals(1, callback.invocations);

        assertTrue(future.isDone());
        try {
            future.get();
            fail("Future should have thrown");
        } catch (ExecutionException e) {
            assertEquals(exception, e.getCause());
        }
    }

    @Test
    public void testBatchCannotAbortTwice() throws Exception {
        ProducerBatch batch = new ProducerBatch(new TopicPartition("topic", 1), memoryRecordsBuilder, now);
        MockCallback callback = new MockCallback();
        FutureRecordMetadata future = batch.tryAppend(now, null, new byte[10], Record.EMPTY_HEADERS, callback, now);
        KafkaException exception = new KafkaException();
        batch.abort(exception);
        assertEquals(1, callback.invocations);
        assertEquals(exception, callback.exception);
        assertNull(callback.metadata);

        try {
            batch.abort(new KafkaException());
            fail("Expected exception from abort");
        } catch (IllegalStateException e) {
            // expected
        }

        assertEquals(1, callback.invocations);
        assertTrue(future.isDone());
        try {
            future.get();
            fail("Future should have thrown");
        } catch (ExecutionException e) {
            assertEquals(exception, e.getCause());
        }
    }

    @Test
    public void testBatchCannotCompleteTwice() throws Exception {
        ProducerBatch batch = new ProducerBatch(new TopicPartition("topic", 1), memoryRecordsBuilder, now);
        MockCallback callback = new MockCallback();
        FutureRecordMetadata future = batch.tryAppend(now, null, new byte[10], Record.EMPTY_HEADERS, callback, now);
        batch.done(500L, 10L, null);
        assertEquals(1, callback.invocations);
        assertNull(callback.exception);
        assertNotNull(callback.metadata);

        try {
            batch.done(1000L, 20L, null);
            fail("Expected exception from done");
        } catch (IllegalStateException e) {
            // expected
        }

        RecordMetadata recordMetadata = future.get();
        assertEquals(500L, recordMetadata.offset());
        assertEquals(10L, recordMetadata.timestamp());
    }

    @Test
    public void testSplitPreservesHeaders() {
        for (CompressionType compressionType : CompressionType.values()) {
            MemoryRecordsBuilder builder = MemoryRecords.builder(
                    ByteBuffer.allocate(1024),
                    MAGIC_VALUE_V2,
                    compressionType,
                    TimestampType.CREATE_TIME,
                    0L);
            ProducerBatch batch = new ProducerBatch(new TopicPartition("topic", 1), builder, now);
            Header header = new RecordHeader("header-key", "header-value".getBytes());

            while (true) {
                FutureRecordMetadata future = batch.tryAppend(
                        now, "hi".getBytes(), "there".getBytes(),
                        new Header[]{header}, null, now);
                if (future == null) {
                    break;
                }
            }
            Deque<ProducerBatch> batches = batch.split(200);
            assertTrue(batches.size() >= 2, "This batch should be split to multiple small batches.");

            for (ProducerBatch splitProducerBatch : batches) {
                for (RecordBatch splitBatch : splitProducerBatch.records().batches()) {
                    for (Record record : splitBatch) {
                        assertTrue(record.headers().length == 1, "Header size should be 1.");
                        assertTrue(record.headers()[0].key().equals("header-key"), "Header key should be 'header-key'.");
                        assertTrue(new String(record.headers()[0].value()).equals("header-value"), "Header value should be 'header-value'.");
                    }
                }
            }
        }
    }

    @Test
    public void testSplitPreservesMagicAndCompressionType() {
        for (byte magic : Arrays.asList(MAGIC_VALUE_V0, MAGIC_VALUE_V1, MAGIC_VALUE_V2)) {
            for (CompressionType compressionType : CompressionType.values()) {
                if (compressionType == CompressionType.NONE && magic < MAGIC_VALUE_V2)
                    continue;

                if (compressionType == CompressionType.ZSTD && magic < MAGIC_VALUE_V2)
                    continue;

                MemoryRecordsBuilder builder = MemoryRecords.builder(ByteBuffer.allocate(1024), magic,
                        compressionType, TimestampType.CREATE_TIME, 0L);

                ProducerBatch batch = new ProducerBatch(new TopicPartition("topic", 1), builder, now);
                while (true) {
                    FutureRecordMetadata future = batch.tryAppend(now, "hi".getBytes(), "there".getBytes(),
                            Record.EMPTY_HEADERS, null, now);
                    if (future == null)
                        break;
                }

                Deque<ProducerBatch> batches = batch.split(512);
                assertTrue(batches.size() >= 2);

                for (ProducerBatch splitProducerBatch : batches) {
                    assertEquals(magic, splitProducerBatch.magic());
                    assertTrue(splitProducerBatch.isSplitBatch());

                    for (RecordBatch splitBatch : splitProducerBatch.records().batches()) {
                        assertEquals(magic, splitBatch.magic());
                        assertEquals(0L, splitBatch.baseOffset());
                        assertEquals(compressionType, splitBatch.compressionType());
                    }
                }
            }
        }
    }

    /**
     * A {@link ProducerBatch} configured using a timestamp preceding its create time is interpreted correctly
     * as not expired by {@link ProducerBatch#hasReachedDeliveryTimeout(long, long)}.
     */
    @Test
    public void testBatchExpiration() {
        long deliveryTimeoutMs = 10240;
        ProducerBatch batch = new ProducerBatch(new TopicPartition("topic", 1), memoryRecordsBuilder, now);
        // Set `now` to 2ms before the create time.
        assertFalse(batch.hasReachedDeliveryTimeout(deliveryTimeoutMs, now - 2));
        // Set `now` to deliveryTimeoutMs.
        assertTrue(batch.hasReachedDeliveryTimeout(deliveryTimeoutMs, now + deliveryTimeoutMs));
    }

    /**
     * A {@link ProducerBatch} configured using a timestamp preceding its create time is interpreted correctly
     * * as not expired by {@link ProducerBatch#hasReachedDeliveryTimeout(long, long)}.
     */
    @Test
    public void testBatchExpirationAfterReenqueue() {
        ProducerBatch batch = new ProducerBatch(new TopicPartition("topic", 1), memoryRecordsBuilder, now);
        // Set batch.retry = true
        batch.reenqueued(now);
        // Set `now` to 2ms before the create time.
        assertFalse(batch.hasReachedDeliveryTimeout(10240, now - 2L));
    }

    @Test
    public void testShouldNotAttemptAppendOnceRecordsBuilderIsClosedForAppends() {
        ProducerBatch batch = new ProducerBatch(new TopicPartition("topic", 1), memoryRecordsBuilder, now);
        FutureRecordMetadata result0 = batch.tryAppend(now, null, new byte[10], Record.EMPTY_HEADERS, null, now);
        assertNotNull(result0);
        assertTrue(memoryRecordsBuilder.hasRoomFor(now, null, new byte[10], Record.EMPTY_HEADERS));
        memoryRecordsBuilder.closeForRecordAppends();
        assertFalse(memoryRecordsBuilder.hasRoomFor(now, null, new byte[10], Record.EMPTY_HEADERS));
        assertNull(batch.tryAppend(now + 1, null, new byte[10], Record.EMPTY_HEADERS, null, now + 1));
    }

    private static class MockCallback implements Callback {
        private int invocations = 0;
        private RecordMetadata metadata;
        private Exception exception;

        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            invocations++;
            this.metadata = metadata;
            this.exception = exception;
        }
    }

}
