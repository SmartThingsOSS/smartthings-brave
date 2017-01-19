
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
import com.github.kristofa.brave.SpanCollectorMetricsHandler;

public class DropwizardSpanCollectorMetricsHandler implements SpanCollectorMetricsHandler {

  private final String ACCEPTED_METER;
  private final String DROPPED_METER;

  private final MetricRegistry registry;

  public DropwizardSpanCollectorMetricsHandler(MetricRegistry registry, String kind) {
    this.registry = registry;

    ACCEPTED_METER = "tracing.collector." + kind + ".span.accepted";
    DROPPED_METER = "tracing.collector." + kind + ".span.dropped";
  }

  @Override
  public void incrementAcceptedSpans(int quantity) {
    registry.meter(ACCEPTED_METER).mark(quantity);
  }

  @Override
  public void incrementDroppedSpans(int quantity) {
    registry.meter(DROPPED_METER).mark(quantity);
  }
}
