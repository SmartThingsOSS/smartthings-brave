
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

import brave.propagation.TraceContextOrSamplingFlags;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class TracedConsumerRecord<K, V> extends ConsumerRecord<K, V>{

  public final TraceContextOrSamplingFlags traceContextOrSamplingFlags;

  public TracedConsumerRecord(ConsumerRecord<K, V> record, TraceContextOrSamplingFlags traceContextOrSamplingFlags) {
    super(record.topic(),
      record.partition(),
      record.offset(),
      record.timestamp(),
      record.timestampType(),
      record.checksum(),
      record.serializedKeySize(),
      record.serializedValueSize(),
      record.key(),
      record.value()
    );
    this.traceContextOrSamplingFlags = traceContextOrSamplingFlags;
  }
}
