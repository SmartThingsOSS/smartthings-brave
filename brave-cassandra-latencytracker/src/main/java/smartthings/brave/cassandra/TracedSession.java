
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
package smartthings.brave.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.LatencyTracker;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.github.kristofa.brave.Brave;
import com.github.kristofa.brave.ServerSpan;
import com.github.kristofa.brave.ServerSpanThreadBinder;
import com.github.kristofa.brave.SpanId;
import com.google.common.base.Optional;
import com.google.common.reflect.AbstractInvocationHandler;
import com.google.common.reflect.Reflection;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.twitter.zipkin.gen.Annotation;
import com.twitter.zipkin.gen.BinaryAnnotation;
import com.twitter.zipkin.gen.Endpoint;
import com.twitter.zipkin.gen.Span;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zipkin.Constants;

import static java.util.Collections.singletonMap;
import static zipkin.internal.Util.checkNotNull;

/**
 * Wraps a Cassandra {@link Session} and writes directly to brave's collector to preserve the
 * correct duration.
 *
 * This is a slightly modified version based on:
 * https://github.com/openzipkin/zipkin/blob/master/zipkin-autoconfigure/storage-cassandra/src/main/java/zipkin/autoconfigure/storage/cassandra/brave/TracedSession.java
 */
public class TracedSession extends AbstractInvocationHandler implements LatencyTracker {

  private static final Logger LOG = LoggerFactory.getLogger(TracedSession.class);

  public static Session create(Session target, Brave brave, String serviceName) {
    return Reflection.newProxy(Session.class, new TracedSession(target, brave, serviceName));
  }

  private final Session target;
  private final Brave brave;
  private final ProtocolVersion version;
  private final String serviceName;

  private final Map<Statement, Span> cache = new ConcurrentHashMap<>();

  private TracedSession(Session target, Brave brave, String serviceName) {
    this.target = checkNotNull(target, "target");
    this.brave = checkNotNull(brave, "brave");
    this.version = target.getCluster().getConfiguration().getProtocolOptions().getProtocolVersion();
    this.serviceName = serviceName;
    target.getCluster().register(this);
  }

  @Override
  public void update(Host host, Statement statement, Exception e, long nanos) {

    runWithSpan(statement, (spanOpt) -> {
      if (spanOpt.isPresent()) {

        Span s = spanOpt.get();
        Endpoint local = s.getAnnotations().get(0).host;
        long timestamp = s.getTimestamp() + nanos / 1000;

        if (e != null) {
          s.addToAnnotations(Annotation.create(timestamp, Constants.ERROR, local));
        } else {
          s.addToAnnotations(Annotation.create(timestamp, "cql.latency_update", local));
        }
      }
      return spanOpt;
    });

  }

  @Override
  protected Object handleInvocation(final Object proxy, final Method method, final Object[] args) throws Throwable {
    boolean traceable = isTraceable(method, args);
    if (traceable) {
      if (method.getName().equals("executeAsync")) {

        return trace(() -> {

          final Statement statement = (Statement) args[0];

          final ListenableFuture<ResultSet> future =
            new TracedResultSetFuture(
              target.executeAsync(statement),
              brave.serverSpanThreadBinder(),
              brave.serverSpanThreadBinder().getCurrentServerSpan());

          Futures.addCallback(future, new FutureCallback<ResultSet>() {
            @Override public void onSuccess(final ResultSet result) {
              runWithSpan(statement, true, (spanOpt -> {
                addHostAnnotations(spanOpt, result);
                return result;
              }));
            }

            @Override public void onFailure(Throwable t) {
              runWithSpan(statement, true, (spanOpt -> {
                addErrorAnnotation(spanOpt, t);
                return null;
              }));
            }
          });

          return future;
        }, as(args[0], NamedBoundStatement.class));

      } else if (method.getName().equals("execute")) {
        return trace(() -> {

          Statement statement = (Statement) args[0];

          return runWithSpan(statement, true, (spanOpt) -> {
            try {
              ResultSet result = target.execute(statement);

              addHostAnnotations(spanOpt, result);

              return result;
            } catch (Exception e) {
              addErrorAnnotation(spanOpt, e);
              throw e;
            }
          });


        }, as(args[0], NamedBoundStatement.class));
      }
    }
    return invokeAndHandleException(method, target, args);
  }

  private <T> T trace(final Supplier<T> f, final NamedBoundStatement statement) {
    if (statement != null) {
      SpanId spanId = brave.clientTracer().startNewSpan(statement.getName());
      // o.a.c.tracing.Tracing.newSession must use the same format for the key zipkin
      if (version.compareTo(ProtocolVersion.V4) >= 0) {
        statement.enableTracing();
        statement.setOutgoingPayload(singletonMap("zipkin", ByteBuffer.wrap(spanId.bytes())));
      }

      brave.clientTracer().setClientSent(); // start the span and store it
      brave.clientTracer().submitBinaryAnnotation("cql.query", statement.preparedStatement().getQueryString());
      brave.clientTracer().submitBinaryAnnotation("cql.keyspace", statement.preparedStatement().getQueryKeyspace());
      brave.clientTracer().submitBinaryAnnotation("cql.read_timeout", String.valueOf(statement.getReadTimeoutMillis()));
      if (statement.getConsistencyLevel() != null) {
        brave.clientTracer().submitBinaryAnnotation("cql.consistency_level", statement.getConsistencyLevel().name());
      }

      cache.put(statement, brave.clientSpanThreadBinder().getCurrentClientSpan());

      // let go of the client span as it is only used for the RPC (will have no local children)
      brave.clientSpanThreadBinder().setCurrentSpan(null);
    }

    return f.get();
  }

  private <R> R runWithSpan(Statement statement, boolean finish, Function<Optional<Span>, R> f) {

    Span span = cache.get(statement);
    if (span != null) {
      Span previous = brave.clientSpanThreadBinder().getCurrentClientSpan();
      brave.clientSpanThreadBinder().setCurrentSpan(span);
      try {
        return f.apply(Optional.of(span));
      } finally {

        if (finish) {
          cache.remove(statement);
          brave.clientTracer().setClientReceived();
        }

        brave.clientSpanThreadBinder().setCurrentSpan(previous);
      }
    } else {
      return f.apply(Optional.absent());
    }


  }

  private <R> R runWithSpan(Statement statement, Function<Optional<Span>, R> f) {
    return runWithSpan(statement, false, f);
  }

  private boolean isTraceable(final Method method, final Object[] args) {

    return args.length > 0 && (
      // Only NamedBoundStatement for now since that will give better span names
      (args[0] instanceof NamedStatement) &&
        // Only join traces, don't start them. This prevents LocalCollector's thread from amplifying.
        brave.serverSpanThreadBinder().getCurrentServerSpan() != null &&
        brave.serverSpanThreadBinder().getCurrentServerSpan().getSpan() != null &&
        ("executeAsync".equals(method.getName()) || "execute".equals(method.getName()))
    );

  }

  private void addErrorAnnotation(Optional<Span> spanOpt, Throwable t) {
    if (spanOpt.isPresent()) {
      Span s = spanOpt.get();
      Endpoint local = (s.getAnnotations().size() > 0) ? s.getAnnotations().get(0).host : null;
      String message = t.getMessage();
      message = message == null ? "unknown" : message;

      s.addToBinary_annotations(BinaryAnnotation.create(Constants.ERROR, message, local));
    }
  }

  private void addHostAnnotations(Optional<Span> spanOpt, ResultSet result) {
    if (spanOpt.isPresent()) {
      Span s = spanOpt.get();
      Host host = result.getExecutionInfo().getQueriedHost();

      int ipv4 = ByteBuffer.wrap(host.getAddress().getAddress()).getInt();
      int port = host.getSocketAddress().getPort();
      Endpoint endpoint = Endpoint.builder().serviceName(serviceName).ipv4(ipv4).port(port).build();

      String cassandraVersion = getOrDefault(host.getCassandraVersion().getBuildLabel());
      String cassandraDataCenter = getOrDefault(host.getDatacenter());
      String cassandraState = getOrDefault(host.getState());
      String cassandraRack = getOrDefault(host.getRack());

      s.addToBinary_annotations(BinaryAnnotation.create("cql.cassandra_version", cassandraVersion, endpoint));
      s.addToBinary_annotations(BinaryAnnotation.create("cql.cassandra_data_center", cassandraDataCenter, endpoint));
      s.addToBinary_annotations(BinaryAnnotation.create("cql.cassandra_rack", cassandraRack, endpoint));
      s.addToBinary_annotations(BinaryAnnotation.create("cql.cassandra_state", cassandraState, endpoint));

      StringBuilder triedHosts = new StringBuilder();
      for (Host h : result.getExecutionInfo().getTriedHosts()) {
        triedHosts.append(h.getAddress().getHostAddress()).append(",");
      }
      triedHosts.deleteCharAt(triedHosts.length()-1);

      s.addToBinary_annotations(BinaryAnnotation.create("cql.cassandra_tried_hosts", triedHosts.toString(), endpoint));

      s.addToBinary_annotations(BinaryAnnotation.address(Constants.SERVER_ADDR, endpoint));
    }
  }

  static String getOrDefault(String value) {
    return getOrDefault(value, "unknown");
  }

  static String getOrDefault(String value, String def) {
    return (value == null || "".equals(value)) ? def : value;
  }

  static Object invokeAndHandleException(Method method, Object target, Object[] args) throws Throwable {
    try {
      return method.invoke(target, args);
    } catch (InvocationTargetException e) {
      if (e.getCause() instanceof RuntimeException) throw e.getCause();
      throw e;
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof TracedSession) {
      TracedSession other = (TracedSession) obj;
      return target.equals(other.target);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return target.hashCode();
  }

  @Override
  public String toString() {
    return target.toString();
  }

  @Override
  public void onRegister(Cluster cluster) {
    // noop
  }

  @Override
  public void onUnregister(Cluster cluster) {
    // noop
  }

  @SuppressWarnings("unchecked")
  private static <T> T as(Object value, Class<T> t) {
    return t.isAssignableFrom(value.getClass()) ? (T)value : null;
  }

  private interface Function<T, R> {
    R apply(T t);
  }

  private interface Supplier<T> {
    T get();
  }

  private static class TracedResultSetFuture implements ResultSetFuture {
    private final ResultSetFuture delegate;
    private final ServerSpanThreadBinder serverSpanThreadBinder;
    private final ServerSpan currentServerSpan;

    TracedResultSetFuture(ResultSetFuture delegate, ServerSpanThreadBinder serverSpanThreadBinder, ServerSpan currentServerSpan) {
      this.delegate = delegate;
      this.serverSpanThreadBinder = serverSpanThreadBinder;
      this.currentServerSpan = currentServerSpan;
    }

    @Override
    public ResultSet getUninterruptibly() {
      return delegate.getUninterruptibly();
    }

    @Override
    public ResultSet getUninterruptibly(long timeout, TimeUnit unit) throws TimeoutException {
      return delegate.getUninterruptibly(timeout, unit);
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      return delegate.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
      return delegate.isCancelled();
    }

    @Override
    public boolean isDone() {
      return delegate.isDone();
    }

    @Override
    public ResultSet get() throws InterruptedException, ExecutionException {
      return delegate.get();
    }

    @Override
    public ResultSet get(long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
      return delegate.get(timeout, unit);
    }

    @Override
    public void addListener(final Runnable listener, Executor executor) {
      delegate.addListener(() -> {

        serverSpanThreadBinder.setCurrentSpan(currentServerSpan);
        try {
          listener.run();
        } finally {
          serverSpanThreadBinder.setCurrentSpan(null);
        }

      }, executor);
    }
  }
}
