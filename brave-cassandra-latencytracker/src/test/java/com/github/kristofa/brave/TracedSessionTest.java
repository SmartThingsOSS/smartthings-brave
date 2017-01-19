
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

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.ProtocolError;
import com.twitter.zipkin.gen.Endpoint;
import com.twitter.zipkin.gen.Span;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.assertj.core.api.Condition;
import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import smartthings.brave.cassandra.NamedPreparedStatement;
import smartthings.brave.cassandra.NamedStatement;
import smartthings.brave.cassandra.TracedSession;
import zipkin.BinaryAnnotation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

public class TracedSessionTest {

  private static final long START_TIME_MICROSECONDS = System.currentTimeMillis() * 1000;

  private static final Endpoint ENDPOINT = Endpoint.create("service", 80);

  private static final zipkin.Endpoint ZIPKIN_ENDPOINT =
    zipkin.Endpoint.create(ENDPOINT.service_name, ENDPOINT.ipv4);

  private static final long TRACE_ID = 105;

  private long timestamp = START_TIME_MICROSECONDS;
  private AnnotationSubmitter.Clock clock = () -> timestamp;

  private Span span = Brave.toSpan(SpanId.builder().spanId(TRACE_ID).sampled(true).build());
  private ServerSpan serverSpan = ServerSpan.create(span);

  @Rule
  public CassandraCQLUnit cassandraCQLUnit =
    new CassandraCQLUnit(new ClassPathCQLDataSet("simple.cql", "test"), null, 30000L);

  private List<zipkin.Span> spans = new ArrayList<>();

  private Brave brave = new Brave.Builder(ENDPOINT).clock(clock).reporter(spans::add).build();


  @After
  public void teardown() {
    spans.clear();
  }

  @Test
  public void should_trace_execute_call() throws Exception {
    brave.serverSpanThreadBinder().setCurrentSpan(serverSpan);

    Session session = TracedSession.create(cassandraCQLUnit.getSession(), brave, "test");

    PreparedStatement statement = NamedPreparedStatement.from(session.prepare("select * from test").setConsistencyLevel(
      ConsistencyLevel.ONE), "select_all");

    ResultSet result = session.execute(statement.bind());

    await().atMost(1, TimeUnit.SECONDS).until(() -> spans.size() == 1);

    assertThat(result.all().size()).isEqualTo(3).describedAs("select returns 3 results");

    assertThat(spans.size()).isEqualTo(1).describedAs("one span recorded");

    zipkin.Span span = spans.get(0);

    BinaryAnnotation cqlQueryAnnotation =
      BinaryAnnotation.create("cql.query", "select * from test", ZIPKIN_ENDPOINT);

    BinaryAnnotation cqlConsistencyAnnotation =
      BinaryAnnotation.create("cql.consistency_level", "ONE", ZIPKIN_ENDPOINT);

    BinaryAnnotation cqlKeyspaceAnnotation =
      BinaryAnnotation.create("cql.keyspace", "test", ZIPKIN_ENDPOINT);

    assertThat(span.binaryAnnotations)
      .haveExactly(1, new Condition<>(b -> b.equals(cqlQueryAnnotation), "cql query"))
      .haveExactly(1, new Condition<>(b -> b.equals(cqlConsistencyAnnotation), "cql consistency level"))
      .haveExactly(1, new Condition<>(b -> b.equals(cqlKeyspaceAnnotation), "cql keyspace"))
      .describedAs("expected binary annotations recorded");

    assertThat(span.annotations)
      .haveExactly(1, new Condition<>(a -> a.value.equals("cr"), "client receive"))
      .haveExactly(1, new Condition<>(a -> a.value.equals("cs"), "client send"))
      .describedAs("expected annotations recorded");
  }

  @Test
  public void should_record_execute_exception() throws Exception {
    brave.serverSpanThreadBinder().setCurrentSpan(serverSpan);

    Session session = TracedSession.create(cassandraCQLUnit.getSession(), brave, "test");

    PreparedStatement
      statement = NamedPreparedStatement.from(session.prepare("select * from test").setConsistencyLevel(
      ConsistencyLevel.ONE), "select_all");

    // trigger exception
    assertThatThrownBy(() -> session.execute(statement.bind().setPagingStateUnsafe(new byte[]{1,2,3}))).isInstanceOf(ProtocolError.class);

    await().atMost(1, TimeUnit.SECONDS).until(() -> spans.size() == 1);

    assertThat(spans.size()).isEqualTo(1).describedAs("one span recorded");

    zipkin.Span span = spans.get(0);

    BinaryAnnotation cqlQueryAnnotation =
      BinaryAnnotation.create("cql.query", "select * from test", ZIPKIN_ENDPOINT);

    BinaryAnnotation cqlConsistencyAnnotation =
      BinaryAnnotation.create("cql.consistency_level", "ONE", ZIPKIN_ENDPOINT);

    BinaryAnnotation cqlKeyspaceAnnotation =
      BinaryAnnotation.create("cql.keyspace", "test", ZIPKIN_ENDPOINT);

    BinaryAnnotation errorAnnotation =
      BinaryAnnotation.create("error", "An unexpected protocol error occurred on host localhost/127.0.0.1:9142. This is a bug in this library, please report: Invalid value for the paging state", ZIPKIN_ENDPOINT);

    assertThat(span.binaryAnnotations)
      .haveExactly(1, new Condition<>(b -> b.equals(cqlQueryAnnotation), "cql query"))
      .haveExactly(1, new Condition<>(b -> b.equals(cqlConsistencyAnnotation), "cql consistency level"))
      .haveExactly(1, new Condition<>(b -> b.equals(cqlKeyspaceAnnotation), "cql keyspace"))
      .haveExactly(1, new Condition<>(b -> b.equals(errorAnnotation), "cql error"))
      .describedAs("expected binary annotations recorded");

    assertThat(span.annotations)
      .haveExactly(1, new Condition<>(a -> a.value.equals("cr"), "client receive"))
      .haveExactly(1, new Condition<>(a -> a.value.equals("cs"), "client send"))
      .describedAs("expected annotations recorded");
  }

  @Test
  public void should_trace_execute_async_call() throws Exception {
    brave.serverSpanThreadBinder().setCurrentSpan(serverSpan);

    Session session = TracedSession.create(cassandraCQLUnit.getSession(), brave, "test");

    PreparedStatement statement = NamedPreparedStatement.from(session.prepare("select * from test").setConsistencyLevel(
      ConsistencyLevel.ONE), "select_all");

    ResultSet result = session.executeAsync(statement.bind()).get(1, TimeUnit.SECONDS);

    assertThat(result.all().size()).isEqualTo(3).describedAs("select returns 3 results");

    await().atMost(1, TimeUnit.SECONDS).until(() -> spans.size() == 1);

    assertThat(spans.size()).isEqualTo(1).describedAs("one span recorded");

    zipkin.Span span = spans.get(0);

    BinaryAnnotation cqlQueryAnnotation =
      BinaryAnnotation.create("cql.query", "select * from test", ZIPKIN_ENDPOINT);

    BinaryAnnotation cqlConsistencyAnnotation =
      BinaryAnnotation.create("cql.consistency_level", "ONE", ZIPKIN_ENDPOINT);

    BinaryAnnotation cqlKeyspaceAnnotation =
      BinaryAnnotation.create("cql.keyspace", "test", ZIPKIN_ENDPOINT);

    assertThat(span.binaryAnnotations)
      .haveExactly(1, new Condition<>(b -> b.equals(cqlQueryAnnotation), "cql query"))
      .haveExactly(1, new Condition<>(b -> b.equals(cqlConsistencyAnnotation), "cql consistency level"))
      .haveExactly(1, new Condition<>(b -> b.equals(cqlKeyspaceAnnotation), "cql keyspace"))
      .describedAs("expected binary annotations recorded");

    assertThat(span.annotations)
      .haveExactly(1, new Condition<>(a -> a.value.equals("cr"), "client receive"))
      .haveExactly(1, new Condition<>(a -> a.value.equals("cs"), "client send"))
      .describedAs("expected annotations recorded");
  }

  @Test
  public void should_record_execute_async_call_exception() throws Exception {
    brave.serverSpanThreadBinder().setCurrentSpan(serverSpan);

    Session session = TracedSession.create(cassandraCQLUnit.getSession(), brave, "test");

    PreparedStatement
      statement = NamedPreparedStatement.from(session.prepare("select * from test").setConsistencyLevel(
      ConsistencyLevel.ONE), "select_all");

    // trigger exception
    assertThatThrownBy(() -> session.executeAsync(statement.bind().setPagingStateUnsafe(new byte[]{1,2,3})).get(1, TimeUnit.SECONDS)).isInstanceOf(ExecutionException.class);

    await().atMost(1, TimeUnit.SECONDS).until(() -> spans.size() == 1);

    assertThat(spans.size()).isEqualTo(1).describedAs("one span recorded");

    zipkin.Span span = spans.get(0);

    BinaryAnnotation cqlQueryAnnotation =
      BinaryAnnotation.create("cql.query", "select * from test", ZIPKIN_ENDPOINT);

    BinaryAnnotation cqlConsistencyAnnotation =
      BinaryAnnotation.create("cql.consistency_level", "ONE", ZIPKIN_ENDPOINT);

    BinaryAnnotation cqlKeyspaceAnnotation =
      BinaryAnnotation.create("cql.keyspace", "test", ZIPKIN_ENDPOINT);

    BinaryAnnotation errorAnnotation =
      BinaryAnnotation.create("error", "An unexpected protocol error occurred on host localhost/127.0.0.1:9142. This is a bug in this library, please report: Invalid value for the paging state", ZIPKIN_ENDPOINT);

    assertThat(span.binaryAnnotations)
      .haveExactly(1, new Condition<>(b -> b.equals(cqlQueryAnnotation), "cql query"))
      .haveExactly(1, new Condition<>(b -> b.equals(cqlConsistencyAnnotation), "cql consistency level"))
      .haveExactly(1, new Condition<>(b -> b.equals(cqlKeyspaceAnnotation), "cql keyspace"))
      .haveExactly(1, new Condition<>(b -> b.equals(errorAnnotation), "cql error"))
      .describedAs("expected binary annotations recorded");

    assertThat(span.annotations)
      .haveExactly(1, new Condition<>(a -> a.value.equals("cr"), "client receive"))
      .haveExactly(1, new Condition<>(a -> a.value.equals("cs"), "client send"))
      .describedAs("expected annotations recorded");


  }
}
