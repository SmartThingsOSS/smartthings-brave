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
package smartthings.brave.asynchttpclient;

import brave.Span;
import brave.http.HttpClientAdapter;
import brave.http.HttpClientHandler;
import brave.http.HttpTracing;
import brave.propagation.CurrentTraceContext;
import brave.propagation.TraceContext;
import io.netty.handler.codec.http.HttpHeaders;
import org.asynchttpclient.AsyncHandler;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.asynchttpclient.HttpResponseBodyPart;
import org.asynchttpclient.HttpResponseHeaders;
import org.asynchttpclient.HttpResponseStatus;
import org.asynchttpclient.Request;
import org.asynchttpclient.RequestBuilder;
import org.asynchttpclient.filter.FilterContext;
import org.asynchttpclient.filter.FilterException;
import org.asynchttpclient.filter.IOExceptionFilter;
import org.asynchttpclient.filter.RequestFilter;
import org.asynchttpclient.filter.ResponseFilter;
import zipkin2.Endpoint;

public class ClientTracing {

  public static DefaultAsyncHttpClientConfig.Builder instrument(
    DefaultAsyncHttpClientConfig.Builder builder, HttpTracing httpTracing) {

    return builder
      .addRequestFilter(new TracingRequestFilter(httpTracing))
      .addResponseFilter(new TracingResponseFilter(httpTracing))
      .addIOExceptionFilter(new TracingIOExceptionFilter(httpTracing));
  }

  private ClientTracing() {}

  private static abstract class TracingFilter {
    final CurrentTraceContext currentTraceContext;
    final TraceContext.Injector<HttpHeaders> injector;
    final HttpClientHandler<Request, HttpResponseStatus> handler;

    TracingFilter(HttpTracing httpTracing) {
      this.currentTraceContext = httpTracing.tracing().currentTraceContext();
      this.injector = httpTracing.tracing().propagation().injector(HttpHeaders::set);
      this.handler = HttpClientHandler.create(httpTracing, new HttpAdapter());
    }
  }

  public static final class TracingRequestFilter extends TracingFilter implements RequestFilter {

    public TracingRequestFilter(HttpTracing httpTracing) {
      super(httpTracing);
    }

    @Override public <T> FilterContext<T> filter(FilterContext<T> ctx) throws FilterException {

      TraceContext parent;
      if (ctx.getAsyncHandler() instanceof AsyncTraceHandler && ((AsyncTraceHandler)ctx.getAsyncHandler()).parent != null) {
        parent = ((AsyncTraceHandler)ctx.getAsyncHandler()).parent;
      } else {
        parent = currentTraceContext.get();
      }

      Span span;
      try (CurrentTraceContext.Scope scope = currentTraceContext.newScope(parent)) {
        span = handler.handleSend(injector, ctx.getRequest().getHeaders(), ctx.getRequest());
      }

      return new FilterContext.FilterContextBuilder<>(ctx)
        .asyncHandler(new AsyncTraceHandler<>(span, parent, handler, ctx.getAsyncHandler()))
        .build();
    }
  }

  public static final class TracingResponseFilter extends TracingFilter implements ResponseFilter {


    public TracingResponseFilter(HttpTracing httpTracing) {
      super(httpTracing);
    }

    @Override public <T> FilterContext<T> filter(FilterContext<T> ctx) throws FilterException {

      if (ctx.getAsyncHandler() instanceof AsyncTraceHandler) {
        AsyncTraceHandler<T> asyncTracingHandler = (AsyncTraceHandler<T>)ctx.getAsyncHandler();
        TraceContext parent = asyncTracingHandler.parent;
        Span span = asyncTracingHandler.span;

        handler.handleReceive(ctx.getResponseStatus(), ctx.getIOException(), span);

        // AHC doesn't call the request filter on a redirect.
        // in order to start a new span for the redirect we need to detect and
        // start it on each receive.
        if (ctx.getResponseStatus().getStatusCode() == 301 || ctx.getResponseStatus().getStatusCode() == 302) {

          // need to send the redirect URL to the tracer handleSend
          String redirectUrl = ctx.getResponseHeaders().getHeaders().get("Location");
          Request dummyRequest = new RequestBuilder(ctx.getRequest()).setUrl(redirectUrl).build();

          try (CurrentTraceContext.Scope scope = currentTraceContext.newScope(parent)) {
            span = handler.handleSend(injector, ctx.getRequest().getHeaders(), dummyRequest);
          }

          return new FilterContext.FilterContextBuilder<>(ctx)
            .asyncHandler(new AsyncTraceHandler<>(span, parent, handler, asyncTracingHandler.delegate))
            .build();

        }
      }

      return ctx;
    }
  }

  public static final class TracingIOExceptionFilter extends TracingFilter implements
    IOExceptionFilter {

    private final HttpTracing httpTracing;

    public TracingIOExceptionFilter(HttpTracing httpTracing) {
      super(httpTracing);
      this.httpTracing = httpTracing;
    }

    @Override public FilterContext filter(FilterContext ctx) throws FilterException {

      if (ctx.getAsyncHandler() instanceof AsyncTraceHandler) {
        AsyncTraceHandler asyncTracingHandler = (AsyncTraceHandler)ctx.getAsyncHandler();
        Span span = asyncTracingHandler.span;

        if (span != null) {
          long timestamp = httpTracing.tracing().clock().currentTimeMicroseconds();
          span.annotate(timestamp, ctx.getIOException().getMessage());
        }
      }

      return ctx;

    }

  }

  private static final class HttpAdapter extends HttpClientAdapter<Request, HttpResponseStatus> {

    @Override public boolean parseServerAddress(Request req, Endpoint.Builder builder) {

      if (req == null) return false;
      if (builder.parseIp(req.getAddress()) || builder.parseIp(req.getUri().getHost())) {
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

    @Override public String requestHeader(Request request, String s) {
      return request.getHeaders().get(s);
    }

    @Override public Integer statusCode(HttpResponseStatus status) {
      return status.getStatusCode();
    }
  }

  /**
   * AsyncHandler that acts as both a carrier of span/parent context as well as handles completion
   * for both error and success.  It is important to note that during our testing the context
   * passed to the {@link ResponseFilter} did not include a transport exception.  It was found that
   * the {@link AsyncHandler#onThrowable(Throwable)} is however called and we can use this as a way
   * to complete the span with an error.
   *
   * The {@link FilterContext<T>} would be a better context carrier except that it doesn't have
   * a way to attach extra data nor can it be extended due to a private constructor.
   *
   * @param <T>
   */
  static final class AsyncTraceHandler<T> implements AsyncHandler<T> {

    private final TraceContext parent;
    private final Span span;
    private final AsyncHandler<T> delegate;
    private final HttpClientHandler<Request, HttpResponseStatus> handler;

    AsyncTraceHandler(Span span, TraceContext parent, HttpClientHandler<Request, HttpResponseStatus> handler, AsyncHandler<T> asyncHandler) {
      this.span = span;
      this.parent = parent;
      this.delegate = asyncHandler;
      this.handler = handler;
    }

    @Override public void onThrowable(Throwable t) {
      this.handler.handleReceive(null, t, span);
      if (this.delegate != null) {
        this.delegate.onThrowable(t);
      }
    }

    @Override public State onBodyPartReceived(HttpResponseBodyPart bodyPart)
      throws Exception {
      if (this.delegate != null) {
        return this.delegate.onBodyPartReceived(bodyPart);
      } else {
        return State.CONTINUE;
      }
    }

    @Override public State onStatusReceived(HttpResponseStatus responseStatus)
      throws Exception {
      if (delegate != null) {
        return this.delegate.onStatusReceived(responseStatus);
      } else {
        return State.CONTINUE;
      }
    }

    @Override public State onHeadersReceived(HttpResponseHeaders headers)
      throws Exception {
      if (delegate != null) {
        return this.delegate.onHeadersReceived(headers);
      } else {
        return State.CONTINUE;
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

}
