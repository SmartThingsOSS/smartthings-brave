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
package smartthings.brave.sqs;

import brave.propagation.Propagation;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import java.util.Map;

public final class AmazonSQSB3Propagation {

  private AmazonSQSB3Propagation() {
  }

  public static final Propagation.Getter<Map<String, MessageAttributeValue>, String> EXTRACTOR =
    (carrier, key) -> {
      if (carrier.containsKey(key)) {
        return carrier.get(key).getStringValue();
      } else {
        return null;
      }
    };

  public static final Propagation.Setter<Map<String, MessageAttributeValue>, String> INJECTOR =
    (carrier, key, v) -> {
      if (v == null) {
        carrier.remove(key);
      } else {
        carrier.put(key,
          new MessageAttributeValue().withDataType("String").withStringValue(v));
      }
    };
}
