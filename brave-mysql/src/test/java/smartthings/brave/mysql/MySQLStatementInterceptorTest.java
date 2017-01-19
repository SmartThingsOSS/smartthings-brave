
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
import com.mysql.jdbc.Connection;
import com.mysql.jdbc.ResultSetInternalMethods;
import com.mysql.jdbc.Statement;
import com.twitter.zipkin.gen.Annotation;
import com.twitter.zipkin.gen.BinaryAnnotation;
import com.twitter.zipkin.gen.Endpoint;
import com.twitter.zipkin.gen.Span;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Properties;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class MySQLStatementInterceptorTest {

  private final ClientRequestInterceptor requestInterceptor = mock(ClientRequestInterceptor.class);
  private final ClientResponseInterceptor responseInterceptor = mock(ClientResponseInterceptor.class);
  private final ServerClientAndLocalSpanState state = mock(ServerClientAndLocalSpanState.class);
  private MySQLStatementInterceptor subject;

  @Before
  public void setUp() throws Exception {
    subject = new MySQLStatementInterceptor();
    MySQLStatementInterceptor.setRequestInterceptor(requestInterceptor);
    MySQLStatementInterceptor.setResponseInterceptor(responseInterceptor);
    MySQLStatementInterceptor.setServerClientAndLocalSpanState(state);
    MySQLStatementInterceptor.setNameProvider(new DefaultQuerySpanNameProvider());
  }

  @Test
  public void preProcessShouldNotFailIfNoClientTracer() throws Exception {
    MySQLStatementInterceptor.setRequestInterceptor(null);
    MySQLStatementInterceptor.setResponseInterceptor(null);
    MySQLStatementInterceptor.setServerClientAndLocalSpanState(null);

    assertNull(subject.preProcess("sql", mock(Statement.class), mock(Connection.class)));

    verifyZeroInteractions(requestInterceptor, responseInterceptor);
  }

  @Test
  public void shouldCreateEndpoint() throws Exception {
    final String schema = randomAlphanumeric(20);
    final String url = "jdbc:mysql://example.com:3306/test?tracingServiceName=test";

    final Connection connection = mock(Connection.class);
    when(connection.getSchema()).thenReturn(schema);
    when(connection.getHost()).thenReturn("example.com");

    final DatabaseMetaData metadata = mock(DatabaseMetaData.class);
    when(connection.getMetaData()).thenReturn(metadata);
    when(metadata.getURL()).thenReturn(url);

    final Endpoint endpoint = MySQLStatementInterceptor.getEndpoint(connection, "test");

    final InOrder order = inOrder(connection, metadata);

    order.verify(connection).getHost();
    order.verify(connection).getMetaData();
    order.verify(metadata).getURL();
    order.verifyNoMoreInteractions();

    assertEquals(1572395042, endpoint.ipv4);
    assertEquals(3306, endpoint.port.intValue());
  }

  @Test
  public void shouldGetServiceNameFromProperties() throws Exception {
    final Properties properties = new Properties();
    properties.put("tracingServiceName", "test-mysql");

    final Connection connection = mock(Connection.class);
    when(connection.getProperties()).thenReturn(properties);

    final String serviceName = MySQLStatementInterceptor.getServiceName(connection);

    final InOrder order = inOrder(connection);

    order.verify(connection).getProperties();

    assertEquals("test-mysql", serviceName);
  }

  @Test
  public void shouldGetServiceNameFromSchemaIfPropertyIsAbsent() throws Exception {
    final Properties properties = new Properties();

    final Connection connection = mock(Connection.class);
    when(connection.getSchema()).thenReturn("schemaname");
    when(connection.getProperties()).thenReturn(properties);

    final String serviceName = MySQLStatementInterceptor.getServiceName(connection);

    final InOrder order = inOrder(connection);

    order.verify(connection).getProperties();
    order.verify(connection).getSchema();

    assertEquals("mysql-schemaname", serviceName);
  }

  @Test
  public void preProcessShouldBeginTracingSQLCall() throws Exception {
    final String sql = randomAlphanumeric(20);
    final String schema = randomAlphanumeric(20);
    final String url = "jdbc:mysql://example.com:3306/test?tracingServiceName=test";

    final Connection connection = mock(Connection.class);
    when(connection.getSchema()).thenReturn(schema);
    when(connection.getHost()).thenReturn("example.com");

    final DatabaseMetaData metadata = mock(DatabaseMetaData.class);
    when(connection.getMetaData()).thenReturn(metadata);
    when(metadata.getURL()).thenReturn(url);

    final Span span = mock(Span.class);
    when(state.sample()).thenReturn(true);
    when(state.getCurrentClientSpan()).thenReturn(span);

    assertNull(subject.preProcess(sql, mock(Statement.class), connection));

    final InOrder order = inOrder(span, state, requestInterceptor);

    order.verify(state).sample();
    order.verify(requestInterceptor).handle(any(ClientRequestAdapter.class));
    order.verify(state).getCurrentClientSpan();
    order.verify(span, times(2)).addToBinary_annotations(any(BinaryAnnotation.class));
    order.verifyNoMoreInteractions();
  }

  @Test
  public void postProcessShouldNotFailIfNoResponseInterceptor() throws Exception {
    MySQLStatementInterceptor.setResponseInterceptor(null);

    assertNull(subject.postProcess("sql", mock(Statement.class), mock(ResultSetInternalMethods.class), mock(Connection.class), 1, true, true, null));

    verifyZeroInteractions(responseInterceptor);
  }

  @Test
  public void postProcessShouldFinishTracingFailedSQLCall() throws Exception {

    final int warningCount = 1;
    final int errorCode = 2;

    final String sql = randomAlphanumeric(20);
    final String schema = randomAlphanumeric(20);
    final String url = "jdbc:mysql://example.com:3306/test?tracingServiceName=test";
    final Properties props = new Properties();
    props.put("tracingServiceName", "test");

    final Connection connection = mock(Connection.class);
    when(connection.getSchema()).thenReturn(schema);
    when(connection.getHost()).thenReturn("example.com");
    when(connection.getProperties()).thenReturn(props);

    final DatabaseMetaData metadata = mock(DatabaseMetaData.class);
    when(connection.getMetaData()).thenReturn(metadata);
    when(metadata.getURL()).thenReturn(url);

    final Endpoint endpoint = MySQLStatementInterceptor.getEndpoint(connection, "test");

    final Span span = mock(Span.class);
    when(state.sample()).thenReturn(true);
    when(state.getCurrentClientSpan()).thenReturn(span);

    assertNull(subject.postProcess(sql, mock(Statement.class), mock(ResultSetInternalMethods.class), connection, warningCount, true, true,
      new SQLException("Bad Squirrel", "123", errorCode)));

    final InOrder order = inOrder(span, state, responseInterceptor);

    order.verify(state).sample();
    order.verify(state).getCurrentClientSpan();
    order.verify(span).addToBinary_annotations(BinaryAnnotation.create("index.used", "false", endpoint));
    order.verify(span).addToBinary_annotations(BinaryAnnotation.create("index.used_good", "false", endpoint));
    order.verify(span).addToAnnotations(any(Annotation.class));
    order.verify(span).addToBinary_annotations(BinaryAnnotation.create("warning.count", "1", endpoint));
    order.verify(span).addToBinary_annotations(BinaryAnnotation.create("error", "Bad Squirrel", endpoint));
    order.verify(span).addToBinary_annotations(BinaryAnnotation.create("error.code", "2", endpoint));
    order.verify(responseInterceptor).handle(any(ClientResponseAdapter.class));

    order.verifyNoMoreInteractions();
  }

  @Test
  public void postProcessShouldFinishTracingSuccessfulSQLCall() throws Exception {

    final int warningCount = 0;
    final String sql = randomAlphanumeric(20);
    final String schema = randomAlphanumeric(20);
    final String url = "jdbc:mysql://example.com:3306/test?tracingServiceName=test";
    final Properties props = new Properties();
    props.put("tracingServiceName", "test");

    final Connection connection = mock(Connection.class);
    when(connection.getSchema()).thenReturn(schema);
    when(connection.getHost()).thenReturn("example.com");
    when(connection.getProperties()).thenReturn(props);

    final DatabaseMetaData metadata = mock(DatabaseMetaData.class);
    when(connection.getMetaData()).thenReturn(metadata);
    when(metadata.getURL()).thenReturn(url);

    final Endpoint endpoint = MySQLStatementInterceptor.getEndpoint(connection, "test");

    final Span span = mock(Span.class);
    when(state.sample()).thenReturn(true);
    when(state.getCurrentClientSpan()).thenReturn(span);

    assertNull(subject.postProcess(sql, mock(Statement.class), mock(ResultSetInternalMethods.class), connection, warningCount, true, true, null));

    final InOrder order = inOrder(span, state, responseInterceptor);

    order.verify(state).sample();
    order.verify(state).getCurrentClientSpan();
    order.verify(span).addToBinary_annotations(BinaryAnnotation.create("index.used", "false", endpoint));
    order.verify(span).addToBinary_annotations(BinaryAnnotation.create("index.used_good", "false", endpoint));
    order.verify(responseInterceptor).handle(any(ClientResponseAdapter.class));

    order.verifyNoMoreInteractions();
  }

}
