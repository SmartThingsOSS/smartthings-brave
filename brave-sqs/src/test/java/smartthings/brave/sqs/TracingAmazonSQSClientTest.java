/**
 * Copyright 2016-2018 SmartThings
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

import brave.Tracer;
import brave.Tracing;
import brave.propagation.CurrentTraceContext;
import brave.propagation.StrictCurrentTraceContext;
import brave.propagation.TraceContext;
import brave.sampler.Sampler;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import java.util.concurrent.ConcurrentLinkedDeque;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import zipkin2.Span;

import static brave.internal.HexCodec.toLowerHex;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

public class TracingAmazonSQSClientTest {

  @Rule
  public AmazonSQSRule sqsRule = new AmazonSQSRule().start(9324);

  private CurrentTraceContext currentTraceContext = new StrictCurrentTraceContext();

  private ConcurrentLinkedDeque<Span> spans = new ConcurrentLinkedDeque<>();

  private AmazonSQS tracingClient;

  private AmazonSQSClientTracing clientTracing;

  private AmazonSQS client = AmazonSQSClient.builder()
    .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("x", "x")))
    .withRegion("us-east-1")
    .build();

  @Before
  public void setup() throws Exception {
    clientTracing = AmazonSQSClientTracing
      .newBuilder(tracingBuilder(Sampler.ALWAYS_SAMPLE).build())
      .remoteServiceName("test-queue").build();

    tracingClient = TracingAmazonSQSClient.create(clientTracing, client);
  }

  @After
  public void close() throws Exception {
    if (client != null) client.shutdown();
  }

  @Test
  public void samplingDisabled() throws Exception {
    clientTracing = AmazonSQSClientTracing
      .create(tracingBuilder(Sampler.NEVER_SAMPLE).build());

    tracingClient = TracingAmazonSQSClient.create(clientTracing, client);
    tracingClient.sendMessage(sqsRule.queueUrl(), "test");

    assertThat(spans.size()).isEqualTo(0);
  }

  @Test
  public void makesChildOfCurrentSpan() throws Exception {
    Tracer tracer = clientTracing.tracing().tracer();

    brave.Span parent = tracer.newTrace().name("test").start();
    try (Tracer.SpanInScope ws = tracer.withSpanInScope(parent)) {
      tracingClient.sendMessage(sqsRule.queueUrl(), "test");
    } finally {
      parent.finish();
    }

    tracingClient.receiveMessage(sqsRule.queueUrl());

    assertThat(spans)
      .extracting(s -> tuple(s.traceId(), s.parentId()))
      .contains(tuple(parent.context().traceIdString(), toLowerHex(parent.context().spanId())));
  }

  @Test
  public void createsOneWaySpanForSendReceive() throws Exception {

    tracingClient.sendMessage(sqsRule.queueUrl(), "test");
    tracingClient.receiveMessage(sqsRule.queueUrl());

    assertThat(spans)
      .extracting(Span::kind)
      .containsExactly(Span.Kind.CLIENT, Span.Kind.SERVER);

    assertThat(spans)
      .extracting(Span::shared)
      .containsOnly(null, true); // server side is sharing the span ID

    assertThat(spans)
      .extracting(Span::duration)
      .containsOnly(null, null); // neither side finished the span
  }

  @Test
  public void setsCorrectSpanNames() throws Exception {
    tracingClient.sendMessage(sqsRule.queueUrl(), "test");
    tracingClient.receiveMessage(sqsRule.queueUrl());

    assertThat(spans)
      .extracting(Span::name)
      .contains("send_message-test");
  }

  private Tracing.Builder tracingBuilder(Sampler sampler) {
    return Tracing.newBuilder()
      .spanReporter(s -> {
        // make sure the context was cleared prior to finish.. no leaks!
        TraceContext current = clientTracing.tracing().currentTraceContext().get();
        if (current != null) {
          assertThat(current.spanId())
            .isNotEqualTo(s.id());
        }
        spans.add(s);
      })
      .currentTraceContext(currentTraceContext)
      .sampler(sampler);
  }

}
