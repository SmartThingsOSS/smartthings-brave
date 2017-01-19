
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


import com.github.kristofa.brave.ClientResponseAdapter;
import com.github.kristofa.brave.ClientResponseInterceptor;
import com.github.kristofa.brave.ClientSpanThreadBinder;
import com.github.kristofa.brave.http.HttpClientResponseAdapter;
import com.github.kristofa.brave.http.HttpResponse;
import com.twitter.zipkin.gen.Annotation;
import com.twitter.zipkin.gen.Endpoint;
import com.twitter.zipkin.gen.Span;

public abstract class AbstractAsyncTracingHandler {

  final ClientResponseInterceptor responseInterceptor;
  final Span span;
  final Endpoint endpoint;
  final ClientSpanThreadBinder spanThreadBinder;

  AbstractAsyncTracingHandler(ClientResponseInterceptor responseInterceptor, ClientSpanThreadBinder spanThreadBinder, Span span, Endpoint endpoint) {
    this.responseInterceptor = responseInterceptor;
    this.span = span;
    this.endpoint = endpoint;
    this.spanThreadBinder = spanThreadBinder;
  }

  synchronized void complete(HttpResponse response) {
    ClientResponseAdapter adapter = new HttpClientResponseAdapter(response);
    if (span != null) {
      spanThreadBinder.setCurrentSpan(span);
    }
    responseInterceptor.handle(adapter);
  }

  void addToAnnotations(Annotation annotation) {
    if (span != null) {
      this.span.addToAnnotations(annotation);
    }
  }
}
