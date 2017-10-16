
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

import brave.Tracing;
import brave.sampler.Sampler;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Before;
import org.junit.Test;
import smartthings.brave.kafka.EnvelopeProtos;
import zipkin2.Endpoint;
import zipkin2.reporter.Reporter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

public class DefaultTracingProducerInterceptorTest {

  private final Reporter<zipkin2.Span> reporter = mock(Reporter.class);
  private final Tracing tracing = Tracing.newBuilder()
    .localServiceName("test")
    .sampler(Sampler.ALWAYS_SAMPLE)
    .traceId128Bit(true)
    .spanReporter(reporter)
    .build();
  private final SpanNameProvider<String> nameProvider = mock(SpanNameProvider.class);
  private final Endpoint endpoint = Endpoint.newBuilder().serviceName("test-service").build();

  private DefaultTracingProducerInterceptor<String> interceptor;

  @Before
  public void setUp() {
    interceptor = new DefaultTracingProducerInterceptor<>();
    interceptor.configure(ImmutableMap.of(
      "brave.tracing", tracing,
      "brave.span.name.provider", nameProvider,
      "brave.span.remote.endpoint", endpoint
    ));
    reset(reporter, nameProvider);
  }

  @Test
  public void testOnSend() throws InvalidProtocolBufferException {
    String topic = "my-topic";
    String key = "ayyy";
    byte[] value = "lmao".getBytes();
    String spanName = "span-name";

    when(nameProvider.spanName(any())).thenReturn(spanName);

    // method under test
    ProducerRecord<String, byte[]> injectedRecord = interceptor
      .onSend(new ProducerRecord<>(topic, null, null, key, value));

    EnvelopeProtos.Envelope envelope = EnvelopeProtos.Envelope.parseFrom(injectedRecord.value());
    assertEquals(topic, injectedRecord.topic());
    assertEquals(null, injectedRecord.partition());

    assertNotNull(envelope.getTraceId());
    assertNotNull(envelope.getTraceIdHigh());
    assertNotNull(envelope.getParentId());
    assertEquals(ByteString.copyFrom(value), envelope.getPayload());
    assertNotNull(envelope.getSpanId());
    assertNotEquals(0, envelope.getSpanId());
  }
}
