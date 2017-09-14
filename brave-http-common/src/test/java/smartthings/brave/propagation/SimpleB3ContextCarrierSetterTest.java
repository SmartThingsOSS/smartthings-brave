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
package smartthings.brave.propagation;

import brave.propagation.Propagation;
import brave.propagation.PropagationSetterTest;
import java.util.Collections;

import static smartthings.brave.propagation.SimpleB3ContextCarrier.FLAGS_NAME;
import static smartthings.brave.propagation.SimpleB3ContextCarrier.PARENT_SPAN_ID_NAME;
import static smartthings.brave.propagation.SimpleB3ContextCarrier.SAMPLED_NAME;
import static smartthings.brave.propagation.SimpleB3ContextCarrier.SPAN_ID_NAME;
import static smartthings.brave.propagation.SimpleB3ContextCarrier.TRACE_ID_NAME;

public class SimpleB3ContextCarrierSetterTest
  extends PropagationSetterTest<SimpleB3ContextCarrier, String> {
  SimpleB3ContextCarrier carrier = SimpleB3ContextCarrier.newBuilder().build();

  @Override public Propagation.KeyFactory<String> keyFactory() {
    return Propagation.KeyFactory.STRING;
  }

  @Override protected SimpleB3ContextCarrier carrier() {
    return carrier;
  }

  @Override protected Propagation.Setter<SimpleB3ContextCarrier, String> setter() {
    return new SimpleB3ContextCarrier.Setter();
  }

  @Override
  protected Iterable<String> read(SimpleB3ContextCarrier carrier, String key) {
    String value = null;
    switch (key) {
      case TRACE_ID_NAME:
        value = carrier.getTraceId();
        if (carrier.getTraceIdHigh() != null) value = carrier.getTraceIdHigh() + value;
        break;
      case SPAN_ID_NAME:
        value = carrier.getSpanId();
        break;
      case PARENT_SPAN_ID_NAME:
        value = carrier.getParentId();
        break;
      case SAMPLED_NAME:
        value = carrier.isSampled() ? "1" : "0";
        break;
      case FLAGS_NAME:
        value = carrier.isDebug() ? "1" : "0";
        break;
    }
    return value != null ? Collections.singletonList(value) : Collections.emptyList();
  }
}
