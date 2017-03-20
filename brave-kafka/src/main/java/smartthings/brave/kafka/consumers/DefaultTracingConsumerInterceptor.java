
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

import brave.propagation.TraceContext;
import brave.propagation.TraceContextOrSamplingFlags;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import smartthings.brave.kafka.EnvelopeProtos;
import smartthings.brave.kafka.producers.DefaultTracingProducerInterceptor;

/**
 * Default {@link BaseTracingConsumerInterceptor} that uses {@link EnvelopeProtos.Envelope} to extract tracing context.
 * See {@link DefaultTracingProducerInterceptor} for the complementary {@link ProducerInterceptor}
 * @param <K> key type
 */
public class DefaultTracingConsumerInterceptor<K> extends BaseTracingConsumerInterceptor<K> {

  @Override
  protected TracedConsumerRecord<K, byte[]> getTracedConsumerRecord(ConsumerRecord<K, byte[]> record) throws ExtractException {
    try {
      EnvelopeProtos.Envelope envelope = EnvelopeProtos.Envelope.parseFrom(record.value());
      TraceContext.Builder builder = TraceContext.newBuilder();
      if (envelope.hasTraceContext()) {
        EnvelopeProtos.TraceContext envelopeCtx = envelope.getTraceContext();
        builder
          .traceIdHigh(envelopeCtx.getTraceIdHigh())
          .traceId(envelopeCtx.getTraceId())
          .spanId(envelopeCtx.getSpanId())
          .shared(envelopeCtx.getShared());
        if (envelopeCtx.hasSampled()) {
          builder.sampled(envelopeCtx.getSampled().getValue());
        }
        if (envelopeCtx.hasParentId()) {
          builder.parentId(envelopeCtx.getParentId().getValue());
        }
      }
      TraceContextOrSamplingFlags traceContextOrSamplingFlags = TraceContextOrSamplingFlags.create(builder);
      ConsumerRecord<K, byte[]> originalRecord = new ConsumerRecord<>(
        record.topic(),
        record.partition(),
        record.offset(),
        record.timestamp(),
        record.timestampType(),
        record.checksum(),
        record.serializedKeySize(),
        record.serializedValueSize(),
        record.key(),
        envelope.getPayload().toByteArray()
      );
      return new TracedConsumerRecord<>(originalRecord, traceContextOrSamplingFlags);
    } catch (InvalidProtocolBufferException e) {
      throw new ExtractException(record, e);
    }
  }
}
