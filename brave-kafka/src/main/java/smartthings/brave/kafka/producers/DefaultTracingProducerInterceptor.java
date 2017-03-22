
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

import brave.propagation.TraceContext;
import com.google.protobuf.BoolValue;
import com.google.protobuf.ByteString;
import com.google.protobuf.Int64Value;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import smartthings.brave.kafka.EnvelopeProtos;
import smartthings.brave.kafka.consumers.DefaultTracingConsumerInterceptor;

/**
 * Default {@link BaseTracingProducerInterceptor} that uses {@link EnvelopeProtos.Envelope} to inject tracing context.
 * see {@link DefaultTracingConsumerInterceptor} for the complementary {@link ConsumerInterceptor}.
 * @param <K>
 */
public class DefaultTracingProducerInterceptor<K> extends BaseTracingProducerInterceptor<K> {

  @Override
  protected ProducerRecord<K, byte[]> getTracedProducerRecord(TraceContext ctx, ProducerRecord<K, byte[]> record) {
    EnvelopeProtos.TraceContext.Builder ctxBuilder = null;
    if (ctx != null) {
      ctxBuilder = EnvelopeProtos.TraceContext.newBuilder()
        .setTraceIdHigh(ctx.traceIdHigh())
        .setTraceId(ctx.traceId())
        .setSpanId(ctx.spanId())
        .setShared(ctx.shared());
      if (ctx.parentId() != null) {
        ctxBuilder.setParentId(Int64Value.newBuilder().setValue(ctx.parentId()).build());
      }
      if (ctx.sampled() != null) {
        ctxBuilder.setSampled(BoolValue.newBuilder().setValue(ctx.sampled()).build());
      }
    }

    EnvelopeProtos.Envelope.Builder builder = EnvelopeProtos.Envelope.newBuilder()
      .setPayload(ByteString.copyFrom(record.value()));
    if (ctxBuilder != null) {
      builder.setTraceContext(ctxBuilder);
    }
    return new ProducerRecord<>(record.topic(), record.partition(), record.timestamp(), record.key(), builder.build().toByteArray());
  }
}
