
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

import brave.Span;
import brave.Tracer;
import brave.Tracing;
import brave.propagation.TraceContext;
import brave.propagation.TraceContextOrSamplingFlags;
import com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import zipkin.Endpoint;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An abstract interceptor that extracts any tracing context and the original record from the received record,
 * annotates and flushes "wr" for each, and finally returns them to the consumer as {@link TracedConsumerRecord}.
 * Extraction is left abstract to match the injection strategy used.
 * Due to the batch nature of {@link ConsumerInterceptor#onConsume(org.apache.kafka.clients.consumer.ConsumerRecords)},
 * this interceptor cannot propagate the trace context for each record.
 * The consumer should be responsible for propagating the extracted context.
 *
 * Required: A {@link Tracer} in config as "brave.tracer".
 * Optional: A {@link SpanNameProvider} in config as "brave.span.name.provider" to customize span name.
 * Optional: A {@link Endpoint} in config as "brave.span.remote.endpoint" to customize span remote endpoint.
 * @param <K> key type
 */
public abstract class BaseTracingConsumerInterceptor<K> implements ConsumerInterceptor<K, byte[]> {
  private Tracing tracing;
  private SpanNameProvider<K> nameProvider;
  private Endpoint kafkaEndpoint;
  private final static Logger logger = Logger.getLogger(BaseTracingConsumerInterceptor.class.getName());

  /**
   * Abstract method for extracting original consumer record and tracing context from a received consumer record,
   * which was potentially mutated by a producer interceptor to have a tracing context injected
   * @param record received records, may have trace context injected
   * @return {@link TracedConsumerRecord}
   * @throws ExtractException
   */
  protected abstract TracedConsumerRecord<K, byte[]> getTracedConsumerRecord(ConsumerRecord<K, byte[]> record)
    throws ExtractException;

  @Override
  public ConsumerRecords<K, byte[]> onConsume(ConsumerRecords<K, byte[]> records) {
    RecordsAccumulator<K> tracedRecords = new RecordsAccumulator<>(records.count());

    for (ConsumerRecord<K, byte[]> record: records) {
      try {
        // Try extracting trace context and original record from received record
        TracedConsumerRecord<K, byte[]> tracedConsumerRecord = getTracedConsumerRecord(record);
        TraceContextOrSamplingFlags traceContextOrSamplingFlags =
          tracedConsumerRecord.traceContextOrSamplingFlags;
        TraceContext ctx = traceContextOrSamplingFlags.context();

        Span span = (ctx != null)
          ? tracing.tracer().joinSpan(ctx)
          : tracing.tracer().newTrace(traceContextOrSamplingFlags.samplingFlags());

        span
          .kind(Span.Kind.SERVER)
          .name(nameProvider.spanName(record))
          .remoteEndpoint(kafkaEndpoint)
          .tag("kafka.partition", String.valueOf(record.partition()))
          .start()
          .flush();
        tracedRecords.addRecord(tracedConsumerRecord);
      } catch (ExtractException e) {
        // TODO this is questionable, maybe let consumer handle?
        logger.log(Level.WARNING, "extract exception, returning unmodified record - " + record, e);
        tracedRecords.addRecord(record);
      }
    }
    return new ConsumerRecords<>(tracedRecords.getRecords());
  }

  @Override
  public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {

  }

  @Override
  public void close() {

  }

  @Override
  public void configure(Map<String, ?> configs) {
    if (configs.get("brave.tracing") == null
      || !(configs.get("brave.tracing") instanceof Tracing)) {
      throw new ConfigException("brave.tracing", configs.get("brave.tracing"), "Must an be instance of brave.Tracing");
    } else {
      tracing = (Tracing) configs.get("brave.tracing");
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


  private static class RecordsAccumulator<K> {
    private final Map<TopicPartition, List<ConsumerRecord<K, byte[]>>> records;

    RecordsAccumulator(int size) {
      records = new HashMap<>(size);
    }

    void addRecord(ConsumerRecord<K, byte[]> record) {
      TopicPartition tp = new TopicPartition(record.topic(), record.partition());
      if (records.containsKey(tp)) {
        records.get(tp).add(record);
      } else {
        List<ConsumerRecord<K, byte[]>> tracedList = new LinkedList<>();
        tracedList.add(record);
        records.put(tp, tracedList);
      }
    }

    Map<TopicPartition, List<ConsumerRecord<K, byte[]>>> getRecords() {
      return ImmutableMap.copyOf(records);
    }
  }
}
