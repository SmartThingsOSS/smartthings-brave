
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
import com.github.kristofa.brave.http.HttpClientRequestAdapter;
import com.github.kristofa.brave.http.SpanNameProvider;
import com.twitter.zipkin.gen.Endpoint;
import com.twitter.zipkin.gen.Span;
import java.util.logging.Logger;

abstract class AbstractTracingRequestFilter<C> {

  final ClientRequestInterceptor requestInterceptor;
  final ClientResponseInterceptor responseInterceptor;
  final SpanNameProvider nameProvider;
  final ClientSpanThreadBinder clientSpanThreadBinder;
  final ServerSpanThreadBinder serverSpanThreadBinder;
  final boolean startTraces;
  final Endpoint endpoint;

  AbstractTracingRequestFilter(ClientRequestInterceptor requestInterceptor,
                              ClientResponseInterceptor responseInterceptor,
                              SpanNameProvider nameProvider,
                              Endpoint endpoint,
                              ClientSpanThreadBinder clientSpanThreadBinder,
                              ServerSpanThreadBinder serverSpanThreadBinder,
                              boolean startTraces) {

    this.requestInterceptor = requestInterceptor;
    this.responseInterceptor = responseInterceptor;
    this.nameProvider = nameProvider;
    this.clientSpanThreadBinder = clientSpanThreadBinder;
    this.serverSpanThreadBinder = serverSpanThreadBinder;
    this.startTraces = startTraces;
    this.endpoint = endpoint;
  }

  abstract Logger getLogger();

  static long currentTimeMicroseconds() {
    return System.currentTimeMillis() * 1000L;
  }

  boolean shouldTrace() {
    // Only join traces, don't start them.
    return startTraces || (serverSpanThreadBinder != null &&
      serverSpanThreadBinder.getCurrentServerSpan() != null &&
      serverSpanThreadBinder.getCurrentServerSpan().getSpan() != null);
  }

  Span startSpan(HttpClientRequest request) {
    HttpClientRequestAdapter adapter = new HttpClientRequestAdapter(request, nameProvider);
    requestInterceptor.handle(adapter);
    Span span = clientSpanThreadBinder.getCurrentClientSpan();
    clientSpanThreadBinder.setCurrentSpan(null);
    return span;
  }

}
