
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

import brave.Tracer;
import brave.propagation.TraceContext;
import brave.propagation.TraceContextOrSamplingFlags;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import smartthings.brave.kafka.EnvelopeProtos;
import zipkin.Constants;
import zipkin.Endpoint;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public abstract class TracingConsumerInterceptor<K> implements ConsumerInterceptor<K, byte[]> {

  protected Tracer tracer;
  protected SpanNameProvider<K> nameProvider;
  protected Endpoint kafkaEndpoint;
  protected final static Logger logger = Logger.getLogger(TracingConsumerInterceptor.class.getName());

  private final TraceContext.Extractor<EnvelopeProtos.Envelope> extractor = new EnvelopeTraceContextExtractor();

  /**
   * Abstract method that transforms the enveloped record to the one consumed by the actual KafkaConsumer
   * @param envelopedRecord enveloped record with tracing info
   * @param ctx joined or new span
   * @param payloadValue original payload data
   * @return record to be consumed by a KafkaConsumer
   */
  abstract protected ConsumerRecord<K, byte[]> transform(
    ConsumerRecord<K, byte[]> envelopedRecord,
    TraceContext ctx,
    byte[] payloadValue) throws IOException;

  @Override
  public ConsumerRecords<K, byte[]> onConsume(ConsumerRecords<K, byte[]> records) {
    Map<TopicPartition, List<ConsumerRecord<K, byte[]>>> unenvelopedRecords = new HashMap<>(records.count());
    records.forEach(record -> {
      try {
        EnvelopeProtos.Envelope envelope = EnvelopeProtos.Envelope.parseFrom(record.value());
        TraceContextOrSamplingFlags traceContextOrSamplingFlags = extractor.extract(envelope);
        TraceContext ctx = traceContextOrSamplingFlags.context();
        (ctx != null ? tracer.joinSpan(ctx) : tracer.newTrace(traceContextOrSamplingFlags.samplingFlags()))
          .name(nameProvider.spanName(record))
          .remoteEndpoint(kafkaEndpoint)
          .tag("kafka.partition", String.valueOf(record.partition()))
          .annotate(Constants.WIRE_RECV)
          .flush();

        ConsumerRecord<K, byte[]> unenvelopedRecord = transform(record, ctx, envelope.getPayload().toByteArray());

        TopicPartition tp = new TopicPartition(record.topic(), record.partition());
        if (unenvelopedRecords.containsKey(tp)) {
          unenvelopedRecords.get(tp).add(unenvelopedRecord);
        } else {
          List<ConsumerRecord<K, byte[]>> list = new LinkedList<>();
          list.add(unenvelopedRecord);
          unenvelopedRecords.put(tp, list);
        }
      } catch (InvalidProtocolBufferException e) {
        logger.log(Level.WARNING, "Invalid envelope protobuf", e);
      } catch (IOException e) {
        logger.log(Level.WARNING, "Failed to transform consumer record", e);
      }
    });
    return new ConsumerRecords<>(unenvelopedRecords);
  }

  @Override
  public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {

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

    if (configs.get("brave.span.remote.endpoint") != null
      && configs.get("brave.span.remote.endpoint") instanceof Endpoint) {
      kafkaEndpoint = (Endpoint) configs.get("brave.span.remote.endpoint");
    } else {
      kafkaEndpoint = Endpoint.builder().serviceName("Kafka").build();
    }
  }
}
