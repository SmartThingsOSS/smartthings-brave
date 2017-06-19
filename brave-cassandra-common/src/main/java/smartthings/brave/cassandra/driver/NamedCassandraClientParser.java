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
package smartthings.brave.cassandra.driver;

import brave.SpanCustomizer;
import brave.cassandra.driver.CassandraClientParser;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Statement;

public class NamedCassandraClientParser extends CassandraClientParser {

  @Override public void request(Statement statement, SpanCustomizer customizer) {
    super.request(statement, customizer);

    if (statement.getConsistencyLevel() != null) {
      customizer.tag("cassandra.consistency_level", statement.getConsistencyLevel().name());
    }
  }

  @Override public void response(ResultSet resultSet, SpanCustomizer customizer) {
    super.response(resultSet, customizer);

    ExecutionInfo executionInfo = resultSet.getExecutionInfo();
    Host host = executionInfo.getQueriedHost();

    customizer.tag("cassandra.version", getOrDefault(host.getCassandraVersion().getBuildLabel()));
    customizer.tag("cassandra.data_center", getOrDefault(host.getDatacenter()));
    customizer.tag("cassandra.rack", getOrDefault(host.getRack()));
    customizer.tag("cassandra.state", getOrDefault(host.getState()));

    StringBuilder triedHosts = new StringBuilder();
    for (Host h : executionInfo.getTriedHosts()) {
      triedHosts.append(h.getAddress().getHostAddress()).append(",");
    }
    triedHosts.deleteCharAt(triedHosts.length()-1);

    customizer.tag("cassandra.tried_hosts", triedHosts.toString());
  }

  @Override protected String spanName(Statement statement) {
    if (statement instanceof NamedStatement) {
      return ((NamedStatement)statement).getName();
    } else {
      return super.spanName(statement);
    }

  }

  private String getOrDefault(String value) {
    return getOrDefault(value, "unknown");
  }

  private String getOrDefault(String value, String def) {
    if (value != null && !"".equals(value)) {
      return value;
    } else {
      return def;
    }
  }

}
