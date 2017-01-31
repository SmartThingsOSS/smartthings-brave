
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
package smartthings.brave.asynchttpclient;

import com.github.kristofa.brave.ClientRequestInterceptor;
import com.github.kristofa.brave.ClientResponseInterceptor;
import com.github.kristofa.brave.ClientSpanThreadBinder;
import com.github.kristofa.brave.ServerSpanThreadBinder;
import com.github.kristofa.brave.http.HttpClientRequest;
import com.github.kristofa.brave.http.HttpResponse;
import com.github.kristofa.brave.http.SpanNameProvider;
import com.twitter.zipkin.gen.Annotation;
import com.twitter.zipkin.gen.Endpoint;
import com.twitter.zipkin.gen.Span;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.asynchttpclient.AsyncHandler;
import org.asynchttpclient.HttpResponseBodyPart;
import org.asynchttpclient.HttpResponseHeaders;
import org.asynchttpclient.HttpResponseStatus;
import org.asynchttpclient.RequestBuilder;
import org.asynchttpclient.filter.FilterContext;
import org.asynchttpclient.filter.FilterException;
import org.asynchttpclient.filter.RequestFilter;
import zipkin.Constants;


public class TracingRequestFilter extends AbstractTracingRequestFilter implements RequestFilter {

  private static final Logger logger = Logger.getLogger(TracingRequestFilter.class.getName());  //LoggerFactory.getLogger(TracingRequestFilter.class);

  public TracingRequestFilter(ClientRequestInterceptor requestInterceptor,
    ClientResponseInterceptor responseInterceptor,
    SpanNameProvider nameProvider,
    Endpoint endpoint,
    ClientSpanThreadBinder clientSpanThreadBinder,
    ServerSpanThreadBinder serverSpanThreadBinder) {
      this(requestInterceptor, responseInterceptor, nameProvider, endpoint,
        clientSpanThreadBinder, serverSpanThreadBinder, true);
  }

  public TracingRequestFilter(ClientRequestInterceptor requestInterceptor,
                              ClientResponseInterceptor responseInterceptor,
                              SpanNameProvider nameProvider,
                              Endpoint endpoint,
                              ClientSpanThreadBinder clientSpanThreadBinder,
                              ServerSpanThreadBinder serverSpanThreadBinder,
                              boolean startNewTraces) {

    super(requestInterceptor, responseInterceptor, nameProvider, endpoint,
      clientSpanThreadBinder, serverSpanThreadBinder, startNewTraces);
  }

  @Override
  Logger getLogger() { return logger; }


  @Override
  public <T> FilterContext<T> filter(FilterContext<T> context) throws FilterException {
    if (shouldTrace()) {
      TracingHttpClientRequest<T> request = new TracingHttpClientRequest<>(context);
      Span span = startSpan(request);
      return request.decorateContext(
        new AsyncTracingHandler<>(responseInterceptor, clientSpanThreadBinder, span, endpoint,
          context.getAsyncHandler())
      );
    } else {
      return context;
    }
  }

  private static class AsyncTracingHandler<T> extends AbstractAsyncTracingHandler implements AsyncHandler<T> {

    private final AsyncHandler<T> handler;
    private HttpResponseStatus status;

    private AsyncTracingHandler(ClientResponseInterceptor responseInterceptor, ClientSpanThreadBinder spanThreadBinder, Span span, Endpoint endpoint, AsyncHandler<T> handler) {
      super(responseInterceptor, spanThreadBinder, span, endpoint);
      this.handler = handler;
    }

    @Override
    public void onThrowable(Throwable t) {
      try {
        this.handler.onThrowable(t);
        addToAnnotations(Annotation.create(currentTimeMicroseconds(), Constants.ERROR, endpoint));
        if (logger.isLoggable(Level.FINEST)) {
          logger.log(Level.FINEST, "async trace handler received throwable", t);
        }
      } finally {
        complete(new TracingHttpClientResponse(status));
      }
    }

    @Override
    public State onBodyPartReceived(HttpResponseBodyPart bodyPart) throws Exception {
      addToAnnotations(Annotation.create(currentTimeMicroseconds(), "Body Part Received", endpoint));
      return this.handler.onBodyPartReceived(bodyPart);
    }

    @Override
    public State onStatusReceived(HttpResponseStatus responseStatus) throws Exception {
      this.status = responseStatus;
      addToAnnotations(Annotation.create(currentTimeMicroseconds(), "Status Received", endpoint));
      return this.handler.onStatusReceived(responseStatus);
    }

    @Override
    public State onHeadersReceived(HttpResponseHeaders headers) throws Exception {
      addToAnnotations(Annotation.create(currentTimeMicroseconds(), "Headers Received", endpoint));
      return this.handler.onHeadersReceived(headers);
    }

    @Override
    public T onCompleted() throws Exception {
      try {
        return handler.onCompleted();
      } finally {
        complete(new TracingHttpClientResponse(status));
      }
    }
  }

  private static class TracingHttpClientResponse implements HttpResponse {

    private final HttpResponseStatus status;

    private TracingHttpClientResponse(HttpResponseStatus status) {
      this.status = status;
    }

    @Override
    public int getHttpStatusCode() {
      return this.status.getStatusCode();
    }
  }

  private static class TracingHttpClientRequest<T> implements HttpClientRequest {

    private final FilterContext<T> context;
    private final Map<String, String> headers;

    TracingHttpClientRequest(FilterContext<T> context) {
      this.context = context;
      this.headers = new HashMap<>();
    }

    private FilterContext<T> decorateContext(AsyncTracingHandler<T> asyncHandler) {
      FilterContext.FilterContextBuilder<T> contextBuilder =
        new FilterContext.FilterContextBuilder<>(context)
          .asyncHandler(asyncHandler);

      if (!headers.isEmpty()) {
        RequestBuilder requestBuilder = new RequestBuilder(context.getRequest());
        for (Map.Entry<String, String> entry : headers.entrySet()) {
          requestBuilder.addHeader(entry.getKey(), entry.getValue());
        }
        contextBuilder.request(requestBuilder.build());
      }

      return contextBuilder.build();
    }

    @Override
    public void addHeader(String name, String value) {
      headers.put(name, value);
    }

    @Override
    public URI getUri() {
      try {
        return context.getRequest().getUri().toJavaNetURI();
      } catch (URISyntaxException e) {
        throw new IllegalStateException(e);
      }
    }

    @Override
    public String getHttpMethod() {
      return context.getRequest().getMethod();
    }
  }

}
