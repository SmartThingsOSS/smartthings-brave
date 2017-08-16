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

import brave.Span;
import brave.Tracer;
import brave.Tracing;
import brave.propagation.TraceContext;
import brave.propagation.TraceContextOrSamplingFlags;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.ResponseMetadata;
import com.amazonaws.regions.Region;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.AddPermissionRequest;
import com.amazonaws.services.sqs.model.AddPermissionResult;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchRequest;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchRequestEntry;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchResult;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityRequest;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityResult;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequest;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.DeleteMessageBatchResult;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteMessageResult;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.DeleteQueueResult;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.ListDeadLetterSourceQueuesRequest;
import com.amazonaws.services.sqs.model.ListDeadLetterSourceQueuesResult;
import com.amazonaws.services.sqs.model.ListQueuesRequest;
import com.amazonaws.services.sqs.model.ListQueuesResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.PurgeQueueRequest;
import com.amazonaws.services.sqs.model.PurgeQueueResult;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.RemovePermissionRequest;
import com.amazonaws.services.sqs.model.RemovePermissionResult;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.SendMessageBatchResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import com.amazonaws.services.sqs.model.SetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.SetQueueAttributesResult;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import zipkin.Endpoint;

public class TracingAmazonSQSClient  implements AmazonSQS {

  public static AmazonSQS create(Tracing tracing, AmazonSQS delegate) {
    return new TracingAmazonSQSClient(AmazonSQSClientTracing.create(tracing), delegate);
  }

  public static AmazonSQS create(AmazonSQSClientTracing clientTracing, AmazonSQS delegate) {
    return new TracingAmazonSQSClient(clientTracing, delegate);
  }

  private final AmazonSQS delegate;
  private final Tracer tracer;
  private final AmazonSQSClientParser parser;
  private final AmazonSQSClientSampler sampler;
  private final String remoteServiceName;
  private final TraceContext.Injector<Map<String, MessageAttributeValue>> injector;
  private final TraceContext.Extractor<Map<String, MessageAttributeValue>> extractor;

  private TracingAmazonSQSClient(AmazonSQSClientTracing tracing, AmazonSQS delegate) {
    super();
    this.delegate = delegate;
    this.tracer = tracing.tracing().tracer();
    this.parser = tracing.parser();
    this.sampler = tracing.sampler();
    String remoteServiceName = tracing.remoteServiceName();
    this.remoteServiceName = remoteServiceName != null
      ? remoteServiceName
      : "amazon-sqs";

    this.injector = tracing.tracing().propagation().injector(AmazonSQSB3Propagation.INJECTOR);
    this.extractor = tracing.tracing().propagation().extractor(AmazonSQSB3Propagation.EXTRACTOR);
  }

  @Override public void setEndpoint(String endpoint) {
    delegate.setEndpoint(endpoint);
  }

  @Override public void setRegion(Region region) {
    delegate.setRegion(region);
  }

  @Override public AddPermissionResult addPermission(AddPermissionRequest addPermissionRequest) {
    return delegate.addPermission(addPermissionRequest);
  }

  @Override public AddPermissionResult addPermission(String queueUrl, String label,
    List<String> aWSAccountIds, List<String> actions) {
    return this.addPermission(new AddPermissionRequest(queueUrl, label, aWSAccountIds, actions));
  }

  @Override public ChangeMessageVisibilityResult changeMessageVisibility(
    ChangeMessageVisibilityRequest changeMessageVisibilityRequest) {
    return delegate.changeMessageVisibility(changeMessageVisibilityRequest);
  }

  @Override public ChangeMessageVisibilityResult changeMessageVisibility(String queueUrl,
    String receiptHandle, Integer visibilityTimeout) {
    return this.changeMessageVisibility(
      new ChangeMessageVisibilityRequest(queueUrl, receiptHandle, visibilityTimeout));
  }

  @Override public ChangeMessageVisibilityBatchResult changeMessageVisibilityBatch(
    ChangeMessageVisibilityBatchRequest changeMessageVisibilityBatchRequest) {
    return delegate.changeMessageVisibilityBatch(changeMessageVisibilityBatchRequest);
  }

  @Override public ChangeMessageVisibilityBatchResult changeMessageVisibilityBatch(String queueUrl,
    List<ChangeMessageVisibilityBatchRequestEntry> entries) {
    return this.changeMessageVisibilityBatch(
      new ChangeMessageVisibilityBatchRequest(queueUrl, entries));
  }

  @Override public CreateQueueResult createQueue(CreateQueueRequest createQueueRequest) {
    return delegate.createQueue(createQueueRequest);
  }

  @Override public CreateQueueResult createQueue(String queueName) {
    return this.createQueue(new CreateQueueRequest(queueName));
  }

  @Override public DeleteMessageResult deleteMessage(DeleteMessageRequest deleteMessageRequest) {
    Span span = tracer.nextSpan().kind(Span.Kind.CLIENT).start();

    try(Tracer.SpanInScope ws = tracer.withSpanInScope(span)) {
      parser.request(deleteMessageRequest, span);
      DeleteMessageResult result = delegate.deleteMessage(deleteMessageRequest);
      parser.response(result, span);
      return result;
    } catch (Exception e) {
      parser.error(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override public DeleteMessageResult deleteMessage(String queueUrl, String receiptHandle) {
    return this.deleteMessage(new DeleteMessageRequest(queueUrl, receiptHandle));
  }

  @Override public DeleteMessageBatchResult deleteMessageBatch(
    DeleteMessageBatchRequest deleteMessageBatchRequest) {
    Span span = tracer.nextSpan().kind(Span.Kind.CLIENT).start();

    try(Tracer.SpanInScope ws = tracer.withSpanInScope(span)) {
      parser.request(deleteMessageBatchRequest, span);
      DeleteMessageBatchResult result = delegate.deleteMessageBatch(deleteMessageBatchRequest);
      parser.response(result, span);
      return result;
    } catch (Exception e) {
      parser.error(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override public DeleteMessageBatchResult deleteMessageBatch(String queueUrl,
    List<DeleteMessageBatchRequestEntry> entries) {
    return this.deleteMessageBatch(new DeleteMessageBatchRequest(queueUrl, entries));
  }

  @Override public DeleteQueueResult deleteQueue(DeleteQueueRequest deleteQueueRequest) {
    return delegate.deleteQueue(deleteQueueRequest);
  }

  @Override public DeleteQueueResult deleteQueue(String queueUrl) {
    return this.deleteQueue(new DeleteQueueRequest(queueUrl));
  }

  @Override public GetQueueAttributesResult getQueueAttributes(
    GetQueueAttributesRequest getQueueAttributesRequest) {
    return delegate.getQueueAttributes(getQueueAttributesRequest);
  }

  @Override
  public GetQueueAttributesResult getQueueAttributes(String queueUrl, List<String> attributeNames) {
    return this.getQueueAttributes(new GetQueueAttributesRequest(queueUrl, attributeNames));
  }

  @Override public GetQueueUrlResult getQueueUrl(GetQueueUrlRequest getQueueUrlRequest) {
    return delegate.getQueueUrl(getQueueUrlRequest);
  }

  @Override public GetQueueUrlResult getQueueUrl(String queueName) {
    return this.getQueueUrl(new GetQueueUrlRequest(queueName));
  }

  @Override public ListDeadLetterSourceQueuesResult listDeadLetterSourceQueues(
    ListDeadLetterSourceQueuesRequest listDeadLetterSourceQueuesRequest) {
    return delegate.listDeadLetterSourceQueues(listDeadLetterSourceQueuesRequest);
  }

  @Override public ListQueuesResult listQueues(ListQueuesRequest listQueuesRequest) {
    return delegate.listQueues(listQueuesRequest);
  }

  @Override public ListQueuesResult listQueues() {
    return this.listQueues(new ListQueuesRequest());
  }

  @Override public ListQueuesResult listQueues(String queueNamePrefix) {
    return this.listQueues(new ListQueuesRequest(queueNamePrefix));
  }

  @Override public PurgeQueueResult purgeQueue(PurgeQueueRequest purgeQueueRequest) {
    return delegate.purgeQueue(purgeQueueRequest);
  }

  @Override
  public ReceiveMessageResult receiveMessage(ReceiveMessageRequest receiveMessageRequest) {
    receiveMessageRequest = receiveMessageRequest.withMessageAttributeNames(
      "X-B3-TraceId", "X-B3-SpanId", "X-B3-ParentSpanId", "X-B3-Sampled", "X-B3-Flags");

    ReceiveMessageResult result = delegate.receiveMessage(receiveMessageRequest);

    // complete in flight one-way spans for all received messages
    for(Message message : result.getMessages()) {
      TraceContextOrSamplingFlags traceContextOrSamplingFlags = extractor.extract(message.getMessageAttributes());
      TraceContext ctx = traceContextOrSamplingFlags.context();
      Span oneWay = withEndpoint((ctx != null)
        ? tracer.joinSpan(ctx)
        : tracer.newTrace(traceContextOrSamplingFlags.samplingFlags()));

      oneWay.kind(Span.Kind.SERVER);
      parser.response(result, oneWay);
      oneWay.annotate("receive-"+parser.spanName(receiveMessageRequest.getQueueUrl()));
      oneWay.start().flush();
    }

    return result;
  }

  @Override public ReceiveMessageResult receiveMessage(String queueUrl) {
    return this.receiveMessage(new ReceiveMessageRequest(queueUrl));
  }

  @Override
  public RemovePermissionResult removePermission(RemovePermissionRequest removePermissionRequest) {
    return delegate.removePermission(removePermissionRequest);
  }

  @Override public RemovePermissionResult removePermission(String queueUrl, String label) {
    return this.removePermission(new RemovePermissionRequest(queueUrl, label));
  }

  @Override public SendMessageResult sendMessage(SendMessageRequest sendMessageRequest) {

    Span oneWay = withEndpoint(tracer.nextSpan())
      .kind(Span.Kind.CLIENT)
      .start();

    injector.inject(oneWay.context(), sendMessageRequest.getMessageAttributes());
    parser.request(sendMessageRequest, oneWay);
    SendMessageResult result = delegate.sendMessage(sendMessageRequest);

    // flush after remote call so we don't start one way spans on a request failure
    oneWay.flush();

    return result;
  }

  @Override public SendMessageResult sendMessage(String queueUrl, String messageBody) {
    return this.sendMessage(new SendMessageRequest(queueUrl, messageBody));
  }

  @Override
  public SendMessageBatchResult sendMessageBatch(SendMessageBatchRequest sendMessageBatchRequest) {

    List<Span> oneWays = new LinkedList<>();
    for (SendMessageBatchRequestEntry entry : sendMessageBatchRequest.getEntries()) {
      Span s = withEndpoint(tracer.nextSpan())
        .kind(Span.Kind.CLIENT)
        .start();

      parser.request(sendMessageBatchRequest, s);
      injector.inject(s.context(), entry.getMessageAttributes());
      oneWays.add(s);
    }

    SendMessageBatchResult result = delegate.sendMessageBatch(sendMessageBatchRequest);

    // flush after success so we don't start one way spans on a request failure.
    for(Span oneWay : oneWays) {
      oneWay.flush();
    }

    return result;
  }

  @Override public SendMessageBatchResult sendMessageBatch(String queueUrl,
    List<SendMessageBatchRequestEntry> entries) {
    return this.sendMessageBatch(new SendMessageBatchRequest(queueUrl, entries));
  }

  @Override public SetQueueAttributesResult setQueueAttributes(
    SetQueueAttributesRequest setQueueAttributesRequest) {
    return delegate.setQueueAttributes(setQueueAttributesRequest);
  }

  @Override public SetQueueAttributesResult setQueueAttributes(String queueUrl,
    Map<String, String> attributes) {
    return this.setQueueAttributes(new SetQueueAttributesRequest(queueUrl, attributes));
  }

  @Override public void shutdown() {
    delegate.shutdown();
  }

  @Override public ResponseMetadata getCachedResponseMetadata(AmazonWebServiceRequest request) {
    return delegate.getCachedResponseMetadata(request);
  }

  private Span withEndpoint(Span span) {
    if (!span.isNoop()) {
      Endpoint.Builder remoteEndpoint = Endpoint.builder().serviceName(remoteServiceName);
      span.remoteEndpoint(remoteEndpoint.build());
    }

    return span;
  }

}
