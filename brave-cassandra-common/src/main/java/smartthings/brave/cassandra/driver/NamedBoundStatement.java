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

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;

public final class NamedBoundStatement extends BoundStatement implements NamedStatement<NamedBoundStatement> {

  private String name;

  /**
   * Creates a new {@code BoundStatement} from the provided prepared
   * statement.
   *
   * @param statement the prepared statement from which to create a {@code BoundStatement}.
   */
  public NamedBoundStatement(PreparedStatement statement) {
    super(statement);
  }

  public NamedBoundStatement(PreparedStatement statement, String name) {
    this(statement);
    this.name = name;
  }

  @Override public String getName() {
    return this.name;
  }

  @Override public NamedBoundStatement withName(String name) {
    this.name = name;
    return this;
  }
}
