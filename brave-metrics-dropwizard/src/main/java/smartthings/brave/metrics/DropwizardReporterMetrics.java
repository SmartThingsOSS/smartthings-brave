
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
package smartthings.brave.metrics;

import com.codahale.metrics.MetricRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zipkin2.reporter.ReporterMetrics;

public final class DropwizardReporterMetrics implements ReporterMetrics {

  private static final Logger logger = LoggerFactory.getLogger(DropwizardReporterMetrics.class);

  private static final String PREFIX = "tracing";

  private final MetricRegistry metricRegistry;
  private final String prefix;

  public DropwizardReporterMetrics(MetricRegistry metricRegistry) {
    this(metricRegistry, PREFIX);
  }

  public DropwizardReporterMetrics(MetricRegistry metricRegistry, String prefix) {
    this.metricRegistry = metricRegistry;
    this.prefix = prefix;
  }

  @Override
  public void incrementMessages() {
    metricRegistry.meter(prefix + ".messages").mark();
  }

  @Override
  public void incrementMessagesDropped(Throwable cause) {
    metricRegistry.meter(prefix + ".messages.dropped").mark();
    logger.warn("Tracing messages dropped", cause);
  }

  @Override
  public void incrementSpans(int quantity) {
    metricRegistry.meter(prefix + ".spans").mark(quantity);
  }

  @Override
  public void incrementSpanBytes(int quantity) {
    metricRegistry.meter(prefix + ".span_bytes").mark(quantity);
  }

  @Override
  public void incrementMessageBytes(int quantity) {
    metricRegistry.meter(prefix + ".message_bytes").mark(quantity);
  }

  @Override
  public void incrementSpansDropped(int quantity) {
    metricRegistry.meter(prefix + ".spans.dropped").mark(quantity);
  }

  @Override
  public void updateQueuedSpans(int update) {
    metricRegistry.meter(prefix + ".queued.spans").mark(update);
  }

  @Override
  public void updateQueuedBytes(int update) {
    metricRegistry.meter(prefix + ".queued.bytes").mark(update);
  }
}
