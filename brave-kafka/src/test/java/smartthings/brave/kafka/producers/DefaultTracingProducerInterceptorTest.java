
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
package smartthings.brave.kafka.producers;

import brave.Tracer;
import brave.propagation.TraceContext;
import brave.sampler.Sampler;
import com.github.kristofa.brave.SpanId;
import com.github.kristofa.brave.SpanUtils;
import com.github.kristofa.brave.TestClientTracer;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Before;
import org.junit.Test;
import smartthings.brave.kafka.EnvelopeProtos;
import zipkin.Endpoint;
import zipkin.Span;
import zipkin.reporter.Reporter;

import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.*;

public class DefaultTracingProducerInterceptorTest {

  private final Reporter<Span> reporter = mock(Reporter.class);
  private final TestClientTracer clientTracer = mock(TestClientTracer.class);
  private final Tracer tracer = Tracer.newBuilder()
    .localServiceName("test")
    .sampler(Sampler.ALWAYS_SAMPLE)
    .traceId128Bit(true)
    .reporter(reporter)
    .build();
  private final SpanNameProvider<String> nameProvider = mock(SpanNameProvider.class);
  private final Endpoint endpoint = Endpoint.builder().serviceName("test-service").build();

  private DefaultTracingProducerInterceptor<String> interceptor;

  @Before
  public void setUp() {
    interceptor = new DefaultTracingProducerInterceptor<>();
    interceptor.configure(ImmutableMap.of(
      "brave.tracer", tracer,
      "brave.client.tracer", clientTracer,
      "brave.span.name.provider", nameProvider,
      "brave.span.remote.endpoint", endpoint
    ));
    reset(clientTracer, reporter, nameProvider);
  }

  @Test
  public void testOnSend() throws InvalidProtocolBufferException {
    String topic = "my-topic";
    String key = "ayyy";
    byte[] value = "lmao".getBytes();
    UUID traceIdWhole = UUID.randomUUID();
    long traceId = traceIdWhole.getLeastSignificantBits();
    long traceIdHigh = traceIdWhole.getMostSignificantBits();
    String spanName = "span-name";
    TraceContext parentTraceContext = TraceContext.newBuilder()
      .traceIdHigh(traceIdHigh)
      .traceId(traceId)
      .spanId(traceId)
      .shared(false)
      .build();
    SpanId parentSpan = SpanUtils.traceContextToSpanId(parentTraceContext);

    when(nameProvider.spanName(any())).thenReturn(spanName);
    when(clientTracer.maybeParent()).thenReturn(parentSpan);


    // method under test
    ProducerRecord<String, byte[]> injectedRecord = interceptor
      .onSend(new ProducerRecord<>(topic, null, null, key, value));

    EnvelopeProtos.Envelope envelope = EnvelopeProtos.Envelope.parseFrom(injectedRecord.value());
    assertEquals(topic, injectedRecord.topic());
    assertEquals(null, injectedRecord.partition());

    assertEquals(traceId, envelope.getTraceContext().getTraceId());
    assertEquals(traceIdHigh, envelope.getTraceContext().getTraceIdHigh());
    assertEquals(traceId, envelope.getTraceContext().getParentId().getValue());
    assertEquals(ByteString.copyFrom(value), envelope.getPayload());
    assertNotEquals(traceId, envelope.getTraceContext().getSpanId());
    assert(envelope.getTraceContext().getSpanId() != 0);
  }
}
