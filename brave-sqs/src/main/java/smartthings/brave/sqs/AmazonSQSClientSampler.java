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

import com.amazonaws.services.sqs.model.SendMessageRequest;
import javax.annotation.Nullable;

public abstract class AmazonSQSClientSampler {

  public static final AmazonSQSClientSampler TRACE_ID = new AmazonSQSClientSampler() {

    @Override public Boolean trySample(SendMessageRequest request) {
      return null;
    }

    @Override public String toString() {
      return "DeferDecision";
    }
  };

  public static final AmazonSQSClientSampler NEVER_SAMPLE = new AmazonSQSClientSampler() {
    @Override public Boolean trySample(SendMessageRequest request) {
      return false;
    }

    @Override public String toString() {
      return "NeverSample";
    }
  };

  @Nullable public abstract Boolean trySample(SendMessageRequest request);

}
