
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
package smartthings.brave.mysql;

import com.github.kristofa.brave.*;
import com.github.kristofa.brave.internal.Nullable;
import com.mysql.jdbc.*;
import com.twitter.zipkin.gen.Annotation;
import com.twitter.zipkin.gen.BinaryAnnotation;
import com.twitter.zipkin.gen.Endpoint;
import com.twitter.zipkin.gen.Span;
import zipkin.Constants;
import zipkin.TraceKeys;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;


public class MySQLStatementInterceptor implements StatementInterceptorV2 {

  private final static String SERVICE_NAME_KEY = "tracingServiceName";

  private static ServerClientAndLocalSpanState state;
  public static void setServerClientAndLocalSpanState(ServerClientAndLocalSpanState state) {
    MySQLStatementInterceptor.state = state;
  }

  private static ClientRequestInterceptor requestInterceptor;
  public static void setRequestInterceptor(ClientRequestInterceptor requestInterceptor) {
    MySQLStatementInterceptor.requestInterceptor = requestInterceptor;
  }

  private static ClientResponseInterceptor responseInterceptor;
  public static void setResponseInterceptor(ClientResponseInterceptor responseInterceptor) {
    MySQLStatementInterceptor.responseInterceptor = responseInterceptor;
  }


  private static QuerySpanNameProvider nameProvider = new DefaultQuerySpanNameProvider();
  public static void setNameProvider(QuerySpanNameProvider provider) {
    nameProvider = provider;
  }


  @Override
  public ResultSetInternalMethods preProcess(final String s, final Statement statement,
                                             final Connection connection) throws SQLException {

    if (requestInterceptor != null && isTracing()) {

      Endpoint endpoint;
      try {
        endpoint = getEndpoint(connection, getServiceName(connection));
      } catch (Exception e) {
        endpoint = null;
      }

      final String sqlToLog;
      // When running a prepared statement, sql will be null and we must fetch the sql from the statement itself
      if (statement instanceof PreparedStatement) {
        sqlToLog = ((PreparedStatement) statement).getPreparedSql();
      } else {
        sqlToLog = s;
      }

      MySQLRequestAdapter adapter = new MySQLRequestAdapter(nameProvider, sqlToLog, endpoint);
      requestInterceptor.handle(adapter);

      Span span = state.getCurrentClientSpan();
      if (span != null) {
        span.addToBinary_annotations(BinaryAnnotation.create(TraceKeys.SQL_QUERY, sqlToLog, endpoint));
        span.addToBinary_annotations(BinaryAnnotation.create("sql.readonly", String.valueOf(connection.isReadOnly()), endpoint));
      }

    }

    return null;
  }

  @Override
  public ResultSetInternalMethods postProcess(final String s, final Statement statement,
                                              final ResultSetInternalMethods resultSetInternalMethods,
                                              final Connection connection, final int warningCount,
                                              final boolean noIndexUsed, final boolean noGoodIndexUsed,
                                              final SQLException statementException) throws SQLException {

    if (responseInterceptor != null && isTracing()) {

      Endpoint endpoint;
      try {
        endpoint = getEndpoint(connection, getServiceName(connection));
      } catch (Exception e) {
        endpoint = null;
      }
      Span span = state.getCurrentClientSpan();
      if (span != null) {
        span.addToBinary_annotations(BinaryAnnotation.create("index.used", String.valueOf(!noIndexUsed), endpoint));
        span.addToBinary_annotations(BinaryAnnotation.create("index.used_good", String.valueOf(!noGoodIndexUsed), endpoint));
        if (warningCount > 0) {
          span.addToAnnotations(Annotation.create(currentTimeMicroseconds(), Constants.ERROR, endpoint));
          span.addToBinary_annotations(BinaryAnnotation.create("warning.count", String.valueOf(warningCount), endpoint));
        }
        if (statementException != null) {
          span.addToBinary_annotations(BinaryAnnotation.create(Constants.ERROR, statementException.getMessage(), endpoint));
          span.addToBinary_annotations(BinaryAnnotation.create("error.code", String.valueOf(statementException.getErrorCode()), endpoint));
        }
      }

      MySQLResponseAdapter adapter = new MySQLResponseAdapter();
      responseInterceptor.handle(adapter);
    }

    return null;
  }

  private boolean isTracing() {
    return state != null && Boolean.TRUE.equals(state.sample());
  }

  static Endpoint getEndpoint(final Connection connection, final String serviceName) throws Exception {
    // infer the host address and port from the connection url
    InetAddress address = Inet4Address.getByName(connection.getHost());
    int ipv4 = ByteBuffer.wrap(address.getAddress()).getInt();
    String urlStr = connection.getMetaData().getURL();
    int i = urlStr.indexOf("mysql:");
    URI url = URI.create(urlStr.substring(i)); // strip up to "mysql:"
    int port = url.getPort() == -1 ? 3306 : url.getPort();
    return Endpoint.create(serviceName, ipv4, port);   //builder().ipv4(ipv4).port((short)port).build();
  }

  static String getServiceName(Connection connection) {
    Properties props = connection.getProperties();
    String serviceName = null;
    if (props != null) {
      serviceName = props.getProperty(SERVICE_NAME_KEY);
      if (serviceName == null || "".equals(serviceName)) {
        serviceName = "mysql";
        String databaseName;
        try {
          databaseName = connection.getSchema();
        } catch (SQLException e) {
          databaseName = null;
        }
        if (databaseName != null && !"".equals(databaseName)) {
          serviceName += "-" + databaseName;
        }
      }
    }
    return serviceName;
  }

  private static long currentTimeMicroseconds() {
    return System.currentTimeMillis() * 1000;
  }

  @Override
  public boolean executeTopLevelOnly() {
    // true ignores queries issued by other interceptors
    return true;
  }

  @Override
  public void init(Connection connection, Properties properties) throws SQLException {
    // noop
  }

  @Override
  public void destroy() {
    // noop
  }

  static class MySQLResponseAdapter implements ClientResponseAdapter {

    private final Collection<KeyValueAnnotation> annotations;

    MySQLResponseAdapter() {
      this(Collections.<KeyValueAnnotation>emptyList());
    }

    MySQLResponseAdapter(Collection<KeyValueAnnotation> annotations) {
      this.annotations = annotations;
    }

    @Override
    public Collection<KeyValueAnnotation> responseAnnotations() {
      return annotations;
    }
  }

  static class MySQLRequestAdapter implements ClientRequestAdapter {

    private final Endpoint endpoint;
    private final QuerySpanNameProvider nameProvider;
    private final String sql;
    private final Collection<KeyValueAnnotation> annotations;

    MySQLRequestAdapter(QuerySpanNameProvider nameProvider, String sql, @Nullable Endpoint endpoint) {
      this(nameProvider, sql, Collections.<KeyValueAnnotation>emptyList(), endpoint);
    }
    MySQLRequestAdapter(QuerySpanNameProvider nameProvider, String sql, Collection<KeyValueAnnotation> annotations, @Nullable Endpoint endpoint) {
      this.nameProvider = nameProvider;
      this.sql = sql;
      this.endpoint = endpoint;
      this.annotations = annotations;
    }

    @Override
    public String getSpanName() {
      return nameProvider.getName(sql);
    }

    @Override
    public void addSpanIdToRequest(@Nullable SpanId spanId) {
      // not supported by mysql
    }

    @Override
    public Collection<KeyValueAnnotation> requestAnnotations() {
      return annotations;
    }

    @Override
    public Endpoint serverAddress() {
      return endpoint;
    }
  }

}
