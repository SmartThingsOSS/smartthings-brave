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
package com.github.kristofa.brave;

import com.github.tomakehurst.wiremock.junit.WireMockClassRule;
import com.twitter.zipkin.gen.Endpoint;
import com.twitter.zipkin.gen.Span;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.assertj.core.api.Condition;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.asynchttpclient.Response;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import smartthings.brave.asynchttpclient.NamedSpanNameProvider;
import smartthings.brave.asynchttpclient.TracingRequestFilter;
import zipkin.BinaryAnnotation;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.assertj.core.api.Assertions.assertThat;

public class TracingRequestFilterTest {

  private static final long START_TIME_MICROSECONDS = System.currentTimeMillis() * 1000;
  private static final Endpoint ENDPOINT = Endpoint.create("service", 80);
  private static final zipkin.Endpoint ZIPKIN_ENDPOINT =
    zipkin.Endpoint.create(ENDPOINT.service_name, ENDPOINT.ipv4);
  private static final long TRACE_ID = 105;

  private long timestamp = START_TIME_MICROSECONDS;
  private AnnotationSubmitter.Clock clock = () -> timestamp;

  private List<zipkin.Span> spans = new ArrayList<>();

  private Brave brave = new Brave.Builder(ENDPOINT).clock(clock).reporter(spans::add).build();

  private TracingRequestFilter filter = new TracingRequestFilter(
    brave.clientRequestInterceptor(),
    brave.clientResponseInterceptor(),
    new NamedSpanNameProvider(),
    ENDPOINT,
    brave.clientSpanThreadBinder(),
    brave.serverSpanThreadBinder(),
    false);

  private AsyncHttpClient client = new DefaultAsyncHttpClient(
    new DefaultAsyncHttpClientConfig.Builder()
      .addRequestFilter(filter).build());

  @ClassRule
  public static WireMockClassRule wireMockClassRule = new WireMockClassRule(8888);

  @Rule
  public WireMockClassRule instanceRule = wireMockClassRule;

  @After
  public void tearDown() {
    spans.clear();
  }

  @Test
  public void should_trace_request_when_sampled() throws Exception {
    Span parentSpan = Brave.toSpan(SpanId.builder().spanId(TRACE_ID).sampled(true).build());
    ServerSpan serverSpan = ServerSpan.create(parentSpan);

    brave.serverSpanThreadBinder().setCurrentSpan(serverSpan);

    stubFor(get(urlEqualTo("/brave/test"))
      .willReturn(aResponse()
        .withStatus(200)
        .withBody("test")));

    Response response = client.prepareGet("http://localhost:8888/brave/test")
      .execute().get(1, TimeUnit.SECONDS);

    assertThat(response.getStatusCode()).isEqualTo(200);
    assertThat(spans.size()).isEqualTo(1);

    BinaryAnnotation urlAnnotation = BinaryAnnotation.create("http.url", "http://localhost:8888/brave/test", ZIPKIN_ENDPOINT);

    zipkin.Span span = spans.get(0);

    assertThat(span.binaryAnnotations)
      .haveExactly(1, new Condition<>(b -> b.equals(urlAnnotation), "http.url"));

  }

  @Test
  public void should_not_trace_request_when_not_sampled() throws Exception {
    Span parentSpan = Brave.toSpan(SpanId.builder().spanId(TRACE_ID).sampled(false).build());
    ServerSpan serverSpan = ServerSpan.create(parentSpan);

    brave.serverSpanThreadBinder().setCurrentSpan(serverSpan);

    stubFor(get(urlEqualTo("/brave/test"))
      .willReturn(aResponse()
        .withStatus(200)
        .withBody("test")));

    Response response = client.prepareGet("http://localhost:8888/brave/test")
      .execute().get(1, TimeUnit.SECONDS);

    assertThat(response.getStatusCode()).isEqualTo(200);
    assertThat(spans.size()).isEqualTo(0);
  }

  @Test
  public void should_not_trace_request_when_server_span_is_absent() throws Exception {

    brave.serverSpanThreadBinder().setCurrentSpan(null);

    stubFor(get(urlEqualTo("/brave/test"))
      .willReturn(aResponse()
        .withStatus(200)
        .withBody("test")));

    Response response = client.prepareGet("http://localhost:8888/brave/test")
      .execute().get(1, TimeUnit.SECONDS);

    assertThat(response.getStatusCode()).isEqualTo(200);
    assertThat(spans.size()).isEqualTo(0);
  }

}
