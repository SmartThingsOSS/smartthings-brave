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
package smartthings.brave.sns;

import brave.Tracing;
import brave.propagation.CurrentTraceContext;
import brave.propagation.StrictCurrentTraceContext;
import brave.propagation.TraceContext;
import com.amazonaws.DefaultRequest;
import com.amazonaws.Request;
import com.amazonaws.Response;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;
import org.junit.Test;
import zipkin2.Span;

import java.util.concurrent.ConcurrentLinkedDeque;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;

public class PublishRequestTracingHandlerTest {

  private CurrentTraceContext currentTraceContext = new StrictCurrentTraceContext();
  private ConcurrentLinkedDeque<Span> spans = new ConcurrentLinkedDeque<>();

  private Tracing tracing = Tracing.newBuilder()
    .spanReporter(s -> {
      // make sure the context was cleared prior to finish.. no leaks!
      TraceContext current = currentTraceContext.get();
      if (current != null) {
        assertThat(current.spanId())
          .isNotEqualTo(s.id());
      }
      spans.add(s);
    })
    .currentTraceContext(currentTraceContext)
    .build();

  private PublishRequestTracingHandler handler = new PublishRequestTracingHandler(tracing);

  @Test
  public void canTracePublish() {
    PublishRequest publishRequest = new PublishRequest("topic", "1234");
    PublishResult publishResult = new PublishResult().withMessageId("54321");

    Request request = new DefaultRequest(publishRequest, "amazon-sns");
    Response response = new Response(publishResult, null);

    handler.beforeMarshalling(publishRequest);
    //do stuff
    handler.afterResponse(request, response);
    assertEquals( 1, spans.size() );
  }

  @Test
  public void canTracePublishError() {
    PublishRequest publishRequest = new PublishRequest("topic", "1234");
    PublishResult publishResult = new PublishResult().withMessageId("54321");

    Request request = new DefaultRequest(publishRequest, "amazon-sns");
    Response response = new Response(publishResult, null);

    handler.afterError(request, response, new Exception("Aw, snap!"));
    assertEquals( 1, spans.size() );
    assertEquals(spans.getFirst().tags().get("error"), "Aw, snap!");
  }
  @Test
  public void canTracePublishRequestError() {
    PublishRequest publishRequest = new PublishRequest("topic", "1234");
    Request request = new DefaultRequest(publishRequest, "amazon-sns");

    handler.afterError(request, null, new Exception("Aw, snap!"));
    assertEquals( 1, spans.size() );
    assertEquals(spans.getFirst().tags().get("error"), "Aw, snap!");
  }

}