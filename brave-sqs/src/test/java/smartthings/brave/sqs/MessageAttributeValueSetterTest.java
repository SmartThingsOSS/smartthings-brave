/**
 * Copyright 2016-2019 SmartThings
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
import brave.test.propagation.PropagationSetterTest;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class MessageAttributeValueSetterTest
  extends PropagationSetterTest<Map<String, MessageAttributeValue>, String> {
  Map<String, MessageAttributeValue> carrier = new LinkedHashMap<>();

  @Override public Propagation.KeyFactory<String> keyFactory() {
    return Propagation.KeyFactory.STRING;
  }

  @Override protected Map<String, MessageAttributeValue> carrier() {
    return carrier;
  }

  @Override protected Propagation.Setter<Map<String, MessageAttributeValue>, String> setter() {
    return AmazonSQSB3Propagation.INJECTOR;
  }

  @Override
  protected Iterable<String> read(Map<String, MessageAttributeValue> carrier, String key) {
    MessageAttributeValue value = carrier.get(key);
    return value != null
      ? Collections.singletonList(value.getStringValue())
      : Collections.emptyList();
  }
}
