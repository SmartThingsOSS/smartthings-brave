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

import brave.Tracing;
import com.google.auto.value.AutoValue;
import javax.annotation.Nullable;

@AutoValue
public abstract class AmazonSQSClientTracing {

  public static AmazonSQSClientTracing create(Tracing tracing) {
    return newBuilder(tracing).build();
  }

  public static Builder newBuilder(Tracing tracing) {
    return new AutoValue_AmazonSQSClientTracing.Builder()
      .tracing(tracing)
      .parser(new AmazonSQSClientParser())
      .sampler(AmazonSQSClientSampler.TRACE_ID);
  }

  public abstract Tracing tracing();

  public abstract AmazonSQSClientParser parser();

  public abstract AmazonSQSClientSampler sampler();

  @Nullable public abstract String remoteServiceName();

  public AmazonSQSClientTracing clientOf(String remoteServiceName) {
    return toBuilder().remoteServiceName(remoteServiceName).build();
  }

  public abstract Builder toBuilder();


  @AutoValue.Builder
  public static abstract class Builder {

    /** @see AmazonSQSClientTracing#tracing() */
    public abstract Builder tracing(Tracing tracing);

    public abstract Builder parser(AmazonSQSClientParser parser);

    public abstract Builder sampler(AmazonSQSClientSampler sampler);

    public abstract AmazonSQSClientTracing build();

    abstract Builder remoteServiceName(@Nullable String remoteServiceName);

    Builder() {}
  }

  AmazonSQSClientTracing() {
  }

}
