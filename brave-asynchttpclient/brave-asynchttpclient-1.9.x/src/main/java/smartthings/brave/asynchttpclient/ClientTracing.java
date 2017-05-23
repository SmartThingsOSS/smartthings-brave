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

import brave.Span;
import brave.http.HttpClientAdapter;
import brave.http.HttpClientHandler;
import brave.http.HttpTracing;
import brave.propagation.CurrentTraceContext;
import brave.propagation.TraceContext;
import com.ning.http.client.AsyncHandler;
import com.ning.http.client.AsyncHttpClientConfig;
import com.ning.http.client.FluentCaseInsensitiveStringsMap;
import com.ning.http.client.HttpResponseBodyPart;
import com.ning.http.client.HttpResponseHeaders;
import com.ning.http.client.HttpResponseStatus;
import com.ning.http.client.Request;
import com.ning.http.client.RequestBuilder;
import com.ning.http.client.filter.FilterContext;
import com.ning.http.client.filter.FilterException;
import com.ning.http.client.filter.IOExceptionFilter;
import com.ning.http.client.filter.RequestFilter;
import com.ning.http.client.filter.ResponseFilter;
import zipkin.Endpoint;

public class ClientTracing {

  public static AsyncHttpClientConfig.Builder instrument(
    AsyncHttpClientConfig.Builder builder, HttpTracing httpTracing) {

    return builder
      .addRequestFilter(new TracingRequestFilter(httpTracing))
      .addResponseFilter(new TracingResponseFilter(httpTracing))
      .addIOExceptionFilter(new TracingIOExceptionFilter(httpTracing));

  }

  private static abstract class TracingFilter {
    protected final CurrentTraceContext currentTraceContext;
    protected final TraceContext.Injector<FluentCaseInsensitiveStringsMap> injector;
    protected final HttpClientHandler<Request, HttpResponseStatus> handler;

    TracingFilter(HttpTracing httpTracing) {
      this.currentTraceContext = httpTracing.tracing().currentTraceContext();
      this.injector = httpTracing.tracing().propagation().injector(FluentCaseInsensitiveStringsMap::add);
      this.handler = HttpClientHandler.create(httpTracing, new HttpAdapter());
    }
  }

  private static final class TracingRequestFilter extends TracingFilter implements RequestFilter {

    TracingRequestFilter(HttpTracing httpTracing) {
      super(httpTracing);
    }

    @Override public <T> FilterContext<T> filter(FilterContext<T> ctx) throws FilterException {

      TraceContext parent;
      if (ctx.getAsyncHandler() instanceof AsyncTracingHandler && ((AsyncTracingHandler)ctx.getAsyncHandler()).parent != null) {
        parent = ((AsyncTracingHandler)ctx.getAsyncHandler()).parent;
      } else {
        parent = currentTraceContext.get();
      }

      Span span;
      try (CurrentTraceContext.Scope scope = currentTraceContext.newScope(parent)) {
        span = handler.handleSend(injector, ctx.getRequest().getHeaders(), ctx.getRequest());
      }

      return new FilterContext.FilterContextBuilder<>(ctx)
        .asyncHandler(new AsyncTracingHandler<>(ctx.getAsyncHandler(), handler, parent, span))
        .build();
    }

  }

  private static final class TracingResponseFilter extends TracingFilter implements ResponseFilter {

    TracingResponseFilter(HttpTracing httpTracing) {
      super(httpTracing);
    }

    @Override public <T> FilterContext<T> filter(FilterContext<T> ctx)
      throws FilterException {

      if (ctx.getAsyncHandler() instanceof AsyncTracingHandler) {
        AsyncTracingHandler<T> asyncTracingHandler = (AsyncTracingHandler<T>)ctx.getAsyncHandler();
        TraceContext parent = asyncTracingHandler.parent;
        Span span = asyncTracingHandler.span;

        handler.handleReceive(ctx.getResponseStatus(), ctx.getIOException(), span);

        // AHC doesn't call the request filter on a redirect.
        // in order to start a new span for the redirect we need to detect and
        // start it on each receive.
        if (ctx.getResponseStatus().getStatusCode() == 301 || ctx.getResponseStatus().getStatusCode() == 302) {

          // need to send the redirect URL to the tracer handleSend
          String redirectUrl = ctx.getResponseHeaders().getHeaders().getFirstValue("Location");
          Request dummyRequest = new RequestBuilder(ctx.getRequest()).setUrl(redirectUrl).build();

          try (CurrentTraceContext.Scope scope = currentTraceContext.newScope(parent)) {
            span = handler.handleSend(injector, ctx.getRequest().getHeaders(), dummyRequest);
          }

          return new FilterContext.FilterContextBuilder<>(ctx)
            .asyncHandler(new AsyncTracingHandler<>(asyncTracingHandler.delegate, handler, parent, span))
            .build();

        }
      }

      return ctx;

    }

  }

  private static final class TracingIOExceptionFilter extends TracingFilter implements IOExceptionFilter {

    private final HttpTracing httpTracing;

    TracingIOExceptionFilter(HttpTracing httpTracing) {
      super(httpTracing);
      this.httpTracing = httpTracing;
    }

    @Override public FilterContext filter(FilterContext ctx) throws FilterException {

      if (ctx.getAsyncHandler() instanceof AsyncTracingHandler) {
        AsyncTracingHandler asyncTracingHandler = (AsyncTracingHandler)ctx.getAsyncHandler();
        Span span = asyncTracingHandler.span;

        if (span != null) {
          long timestamp = httpTracing.tracing().clock().currentTimeMicroseconds();
          span.annotate(timestamp, ctx.getIOException().getMessage());
        }
      }

      return ctx;

    }

  }

  private static final class AsyncTracingHandler<T> implements AsyncHandler<T> {

    private final AsyncHandler<T> delegate;
    private final TraceContext parent;
    private final Span span;
    protected final HttpClientHandler<Request, HttpResponseStatus> handler;

    AsyncTracingHandler(AsyncHandler<T> delegate,
      HttpClientHandler<Request, HttpResponseStatus> handler,
      TraceContext parent, Span span) {
      this.delegate = delegate;
      this.parent = parent;
      this.span = span;
      this.handler = handler;
    }

    @Override public void onThrowable(Throwable t) {
      this.handler.handleReceive(null, t, span);
      if (delegate != null) {
        this.delegate.onThrowable(t);
      }
    }

    @Override public STATE onBodyPartReceived(HttpResponseBodyPart bodyPart)
      throws Exception {

      if (delegate != null) {
        return this.delegate.onBodyPartReceived(bodyPart);
      } else {
        return STATE.CONTINUE;
      }
    }

    @Override public STATE onStatusReceived(HttpResponseStatus responseStatus)
      throws Exception {
      if (delegate != null) {
        return this.delegate.onStatusReceived(responseStatus);
      } else {
        return STATE.CONTINUE;
      }
    }

    @Override public STATE onHeadersReceived(HttpResponseHeaders headers)
      throws Exception {
      if (delegate != null) {
        return this.delegate.onHeadersReceived(headers);
      } else {
        return STATE.CONTINUE;
      }
    }

    @Override public T onCompleted() throws Exception {
      if (delegate != null) {
        return this.delegate.onCompleted();
      } else {
        return null;
      }
    }
  }

  private static final class HttpAdapter extends HttpClientAdapter<Request, HttpResponseStatus> {

    @Override public boolean parseServerAddress(Request req, Endpoint.Builder builder) {

      if (req == null) return false;
      if (builder.parseIp(req.getInetAddress()) || builder.parseIp(req.getUri().getHost())) {
        builder.port(req.getUri().getPort());
        return true;
      }
      return false;
    }

    @Override public String method(Request request) {
      return request.getMethod();
    }

    @Override public String url(Request request) {
      return request.getUrl();
    }

    @Override public String requestHeader(Request request, String name) {
      return request.getHeaders().getFirstValue(name);
    }

    @Override public Integer statusCode(HttpResponseStatus response) {
      return response.getStatusCode();
    }
  }

}
