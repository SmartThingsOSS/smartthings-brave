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

import brave.Span;
import brave.Tracing;
import brave.propagation.TraceContext;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.Request;
import com.amazonaws.Response;
import com.amazonaws.handlers.RequestHandler2;
import com.amazonaws.services.sns.model.MessageAttributeValue;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;

import zipkin2.Endpoint;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.logging.Logger;

/**
 * AWS RequestHandler2 that adds tracing around the afterError and afterResponse events
 * when calling the AWS SNS publish API.
 */
public class PublishRequestTracingHandler extends RequestHandler2 {

  private static final String SNS_TOPIC_ARN = "sns.topic_arn";
  private static final String SNS_MESSAGE_ID = "sns.msg_id";

  private static final String SERVICE_NAME = "AmazonSNS";

  private static final Logger logger = Logger.getLogger(PublishRequestTracingHandler.class.getName());

  protected Tracing tracing;
  protected final TraceContext.Injector<Map<String, MessageAttributeValue>> injector;
  protected final TraceContext.Extractor<Map<String, MessageAttributeValue>> extractor;

  public PublishRequestTracingHandler(Tracing tracing) {
    this.tracing = tracing;
    this.injector = tracing.propagation().injector(AmazonSNSB3Propagation.INJECTOR);
    this.extractor = tracing.propagation().extractor(AmazonSNSB3Propagation.EXTRACTOR);
  }

  @Override
  public void afterError(Request<?> request, Response<?> response, Exception e) {
    if (request.getOriginalRequestObject() instanceof PublishRequest) {
      Span span =  tracing.tracer().nextSpan()
        .remoteEndpoint(Endpoint.newBuilder().serviceName(SERVICE_NAME).build())
        .kind(Span.Kind.CLIENT)
        .start();

      span.tag("error", e.getMessage());
      span.flush();
    }
  }

  @Override
  public AmazonWebServiceRequest beforeMarshalling(AmazonWebServiceRequest request) {
    if (request instanceof PublishRequest) {
      PublishRequest publishRequest = (PublishRequest) request;

      Span oneWay = tracing.tracer().nextSpan()
        .remoteEndpoint(Endpoint.newBuilder().serviceName(SERVICE_NAME).build())
        .kind(Span.Kind.CLIENT)
        .start();

      injector.inject(oneWay.context(), publishRequest.getMessageAttributes());

      String name = "unknown_topic";
      if (publishRequest.getTopicArn() != null) {
        name = publishRequest.getTopicArn();
      }
      oneWay.name(name);
      oneWay.tag(SNS_TOPIC_ARN, name);

      tracing.tracer().withSpanInScope(oneWay);
    }
    return request;
  }

  @Override
  public void afterResponse(Request<?> request, Response<?> response) {
    if (response.getAwsResponse() instanceof PublishResult) {
      PublishResult publishResult = (PublishResult) response.getAwsResponse();

      Span span = tracing.tracer().currentSpan();

      if (publishResult != null && publishResult.getMessageId() != null) {
        span.tag(SNS_MESSAGE_ID, publishResult.getMessageId());
        span.flush();
      }
    }
  }
}
