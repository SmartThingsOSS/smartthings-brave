
/**
 * Copyright 2016-2017 SmartThings
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package smartthings.brave.kafka.consumers;

import brave.Tracing;
import brave.propagation.TraceContext;
import brave.sampler.Sampler;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.Int64Value;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import smartthings.brave.kafka.EnvelopeProtos;
import zipkin.reporter.Reporter;
import zipkin2.Endpoint;
import zipkin2.Span;

import java.util.UUID;

import static brave.internal.HexCodec.toLowerHex;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;


public class DefaultTracingConsumerInterceptorTest {

  private final Reporter<Span> reporter = mock(Reporter.class);
  private final Tracing tracing = Tracing.newBuilder()
    .localServiceName("test")
    .sampler(Sampler.ALWAYS_SAMPLE)
    .traceId128Bit(true)
    .spanReporter(reporter)
    .build();
  private final SpanNameProvider<String> nameProvider = mock(SpanNameProvider.class);
  private final Endpoint endpoint = Endpoint.newBuilder().serviceName("test-service").build();
  private final ArgumentCaptor<Span> spanCaptor = ArgumentCaptor.forClass(Span.class);

  private DefaultTracingConsumerInterceptor<String> interceptor;

  @Before
  public void setUp() {
    interceptor = new DefaultTracingConsumerInterceptor<>();
    interceptor.configure(ImmutableMap.of(
      "brave.tracing", tracing,
      "brave.span.name.provider", nameProvider,
      "brave.span.remote.endpoint", endpoint
    ));
    reset(reporter, nameProvider);
  }

  @Test
  public void testOnConsume() throws InvalidProtocolBufferException {
    String topic = "my-topic";
    int partition = 1;
    long offset = 1337;
    String key = "ayyy";
    byte[] value = "lmao".getBytes();
    UUID traceIdWhole = UUID.randomUUID();
    long traceId = traceIdWhole.getLeastSignificantBits();
    long traceIdHigh = traceIdWhole.getMostSignificantBits();
    Long parentId = UUID.randomUUID().getLeastSignificantBits();
    long spanId = UUID.randomUUID().getLeastSignificantBits();
    String spanName = "span-name";

    TraceContext expectedTraceContext = TraceContext.newBuilder()
      .traceIdHigh(traceIdHigh)
      .traceId(traceId)
      .parentId(parentId)
      .spanId(spanId)
      .shared(false)
      .build();

    EnvelopeProtos.Envelope envelope = EnvelopeProtos.Envelope.newBuilder()
      .setTraceId(traceId)
      .setTraceIdHigh(traceIdHigh)
      .setParentId(Int64Value.newBuilder().setValue(parentId))
      .setSpanId(spanId)
      .setShared(false)
      .setPayload(ByteString.copyFrom(value))
      .build();

    when(nameProvider.spanName(any())).thenReturn(spanName);

    // method under test
    ConsumerRecords<String, byte[]> records = interceptor.onConsume(new ConsumerRecords<>(ImmutableMap.of(
      new TopicPartition(topic, partition),
      ImmutableList.of(new ConsumerRecord<>(topic, partition, offset, key, envelope.toByteArray()))
    )));

    assertEquals(1, records.count());
    ConsumerRecord<String, byte[]> record = records.iterator().next();
    assertEquals(topic, record.topic());
    assertEquals(partition, record.partition());
    assertEquals(key, record.key());

    assertTrue("record is not a TracedConsumerRecord", record instanceof TracedConsumerRecord);
    TracedConsumerRecord tracedRecord = ((TracedConsumerRecord) record);
    assertNull(tracedRecord.traceContextOrSamplingFlags.samplingFlags());

    assertEquals(expectedTraceContext, tracedRecord.traceContextOrSamplingFlags.context());

    verify(reporter).report(spanCaptor.capture());
    Span capturedSpan = spanCaptor.getValue();
    assertEquals(toLowerHex(spanId), capturedSpan.id());
    assertEquals(toLowerHex(parentId), capturedSpan.parentId());
    assertEquals(toLowerHex(traceIdHigh, traceId), capturedSpan.traceId());
    assertEquals(Span.Kind.SERVER, capturedSpan.kind());
  }
}
