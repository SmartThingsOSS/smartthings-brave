
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

import brave.Span;
import brave.Tracer;
import brave.propagation.TraceContext;
import com.github.kristofa.brave.ClientTracer;
import com.github.kristofa.brave.SpanIdUtils;
import com.google.protobuf.ByteString;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.ConfigException;
import smartthings.brave.kafka.EnvelopeProtos;
import zipkin.Constants;
import zipkin.Endpoint;

import java.util.Map;

/**
 * Uses {@link Tracer} to create one-way {@link brave.Span#flush()} span (With "cs" and "sr" only)
 * @param <K>
 */
public class TracingProducerInterceptor<K> implements ProducerInterceptor<K, byte[]> {

  private Tracer tracer;
  private ClientTracer clientTracer;
  private SpanNameProvider<K> nameProvider;
  private Endpoint kafkaEndpoint;

  @Override
  public ProducerRecord<K, byte[]> onSend(ProducerRecord<K, byte[]> record) {
    Span span = SpanIdUtils.getNextSpan(clientTracer, tracer)
      .name(nameProvider.spanName(record))
      .annotate(Constants.WIRE_SEND)
      .remoteEndpoint(kafkaEndpoint);
    if (record.partition() != null) {
      span.tag("Partition", record.partition().toString());
    }
    TraceContext ctx = span.context();

    // Set trace context of the envelope
    EnvelopeProtos.Envelope.Builder builder = EnvelopeProtos.Envelope.newBuilder()
      .setTraceIdHigh(ctx.traceIdHigh())
      .setTraceId(ctx.traceId())
      .setSpanId(ctx.spanId())
      .setShared(ctx.shared())
      .setPayload(ByteString.copyFrom(record.value()));
    if (ctx.parentId() != null) {
      builder.setParentId(ctx.parentId());
    }
    if (ctx.sampled() != null) {
      builder.setSampled(ctx.sampled());
    }
    EnvelopeProtos.Envelope envelope = builder.build();
    span.flush();
    return new ProducerRecord<>(record.topic(), record.partition(), record.timestamp(), record.key(), envelope.toByteArray());
  }

  @Override
  public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

  }

  @Override
  public void close() {

  }

  @Override
  public void configure(Map<String, ?> configs) {
    if (configs.get("brave.tracer") == null
      || !(configs.get("brave.tracer") instanceof Tracer)) {
      throw new ConfigException("brave.tracer", configs.get("brave.tracer"), "Must an be instance of brave.Tracer");
    } else {
      tracer = (Tracer) configs.get("brave.tracer");
    }

    if (configs.get("brave.span.name.provider") != null
      && configs.get("brave.span.name.provider") instanceof SpanNameProvider) {
      nameProvider = (SpanNameProvider<K>) configs.get("brave.span.name.provider");
    } else {
      nameProvider = new DefaultSpanNameProvider<>();
    }

    if (configs.get("brave.client.tracer") != null
      && configs.get("brave.client.tracer") instanceof ClientTracer) {
      clientTracer = (ClientTracer) configs.get("brave.client.tracer");
    }

    if (configs.get("brave.span.remote.endpoint") != null
      && configs.get("brave.span.remote.endpoint") instanceof Endpoint) {
      kafkaEndpoint = (Endpoint) configs.get("brave.span.remote.endpoint");
    } else {
      kafkaEndpoint = Endpoint.builder().serviceName("Kafka").build();
    }
  }
}
