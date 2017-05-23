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
import com.google.common.reflect.AbstractInvocationHandler;
import com.google.common.reflect.Reflection;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public final class NamedPreparedStatement extends AbstractInvocationHandler implements NamedStatement<NamedPreparedStatement> {

  public static PreparedStatement from(PreparedStatement statement, String name) {
    return Reflection.newProxy(PreparedStatement.class, new NamedPreparedStatement(statement, name));
  }

  private final PreparedStatement target;
  private String name;

  private NamedPreparedStatement(PreparedStatement target, String name) {
    this.target = target;
    this.name = name;
  }

  @Override
  public String getName() {
    return this.name;
  }

  @Override
  public NamedPreparedStatement withName(String name) {
    this.name = name;
    return this;
  }

  @Override
  protected Object handleInvocation(Object proxy, Method method, Object[] args) throws Throwable {
    if (method.getName().equals("bind")) {
      BoundStatement statement = new NamedBoundStatement(target).withName(name);
      if (args.length == 1) {
        Object[] varargs = (Object[])args[0];
        return statement.bind(varargs);
      } else {
        return statement;
      }
    }

    try {
      return method.invoke(target, args);
    } catch (InvocationTargetException e) {
      if (e.getCause() instanceof RuntimeException) throw e.getCause();
      throw e;
    }
  }

}
