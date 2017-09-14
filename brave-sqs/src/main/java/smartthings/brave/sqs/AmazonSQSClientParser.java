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

import brave.SpanCustomizer;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequest;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.DeleteMessageBatchResult;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteMessageResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchResult;
import com.amazonaws.services.sqs.model.SendMessageBatchResultEntry;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import java.net.MalformedURLException;
import java.net.URL;

public class AmazonSQSClientParser {

  public void request(SendMessageRequest request, SpanCustomizer customizer) {
    customizer.name(spanName(request));
    customizer.tag(AmazonSQSTraceKeys.SQS_QUEUE_URL, request.getQueueUrl());
  }

  public void request(SendMessageBatchRequest request, SpanCustomizer customizer) {
    customizer.name(spanName(request));
    customizer.tag(AmazonSQSTraceKeys.SQS_QUEUE_URL, request.getQueueUrl());
  }

  public void request(ReceiveMessageRequest request, SpanCustomizer customizer) {
    customizer.name(spanName(request));
    customizer.tag(AmazonSQSTraceKeys.SQS_QUEUE_URL, request.getQueueUrl());
  }

  public void request(DeleteMessageRequest request, SpanCustomizer customizer) {
    customizer.name(spanName(request));
    customizer.tag(AmazonSQSTraceKeys.SQS_QUEUE_URL, request.getQueueUrl());
    customizer.tag(AmazonSQSTraceKeys.SQS_RECEIPT_HANDLE, request.getReceiptHandle());
  }

  public void request(DeleteMessageBatchRequest request, SpanCustomizer customizer) {
    customizer.name(spanName(request));
    customizer.tag(AmazonSQSTraceKeys.SQS_QUEUE_URL, request.getQueueUrl());
    for(DeleteMessageBatchRequestEntry entry : request.getEntries()) {
      customizer.tag(AmazonSQSTraceKeys.SQS_RECEIPT_HANDLE, entry.getReceiptHandle());
    }
  }

  public void response(SendMessageResult result, SpanCustomizer customizer) {
    customizer.tag(AmazonSQSTraceKeys.SQS_MESSAGE_ID, result.getMessageId());
  }

  public void response(SendMessageBatchResult result, SpanCustomizer customizer) {
    for(SendMessageBatchResultEntry entry : result.getSuccessful()) {
      customizer.tag(AmazonSQSTraceKeys.SQS_MESSAGE_ID, entry.getMessageId());
    }
  }

  public void response(ReceiveMessageResult result, SpanCustomizer customizer) {
    for (Message message : result.getMessages()) {
      customizer.tag(AmazonSQSTraceKeys.SQS_MESSAGE_ID, message.getMessageId());
    }
  }

  public void response(DeleteMessageResult result, SpanCustomizer customizer) {
  }

  public void response(DeleteMessageBatchResult result, SpanCustomizer customizer) {
  }

  public void error(Throwable throwable, SpanCustomizer customizer) {
    String message = throwable.getMessage();
    customizer.tag("error", message);
  }

  protected String spanName(SendMessageBatchRequest request) {
    return "send_message_batch-" + spanName(request.getQueueUrl());
  }

  protected String spanName(SendMessageRequest request) {
    return "send_message-" + spanName(request.getQueueUrl());
  }

  protected String spanName(ReceiveMessageRequest request) {
    return "receive_message-" + spanName(request.getQueueUrl());
  }

  protected String spanName(DeleteMessageRequest request) {
    return "delete_message-" + spanName(request.getQueueUrl());
  }

  protected String spanName(DeleteMessageBatchRequest request) {
    return "delete_message_batch-" + spanName(request.getQueueUrl());
  }

  protected String spanName(String queueUrl) {
    try {
      return spanName(new URL(queueUrl));
    } catch (MalformedURLException e) {
      return "malformed";
    }
  }

  protected String spanName(URL queueUrl) {
    return queueUrl.getPath().split("/")[2];
  }

}
