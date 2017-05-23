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
package smartthings.brave.http;

import brave.http.HttpAdapter;
import brave.http.HttpServerParser;
import java.util.regex.Pattern;

public class SanitizingHttpServerParser extends HttpServerParser {

  private static final Pattern EMAIL_PATTERN =
    Pattern.compile("[A-Z0-9._%+-]+@[A-Z0-9.-]+\\.[A-Z]{2,6}", Pattern.CASE_INSENSITIVE);
  private static final Pattern UUID_PATTERN =
    Pattern.compile("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", Pattern.CASE_INSENSITIVE);

  protected <Req> String spanName(HttpAdapter<Req, ?> adapter, Req req) {
    return adapter.method(req) + " " + sanitizedUrl(adapter.path(req));
  }

  private String sanitizedUrl(String path) {
    path = UUID_PATTERN.matcher(path).replaceAll("_uuid_");
    path = EMAIL_PATTERN.matcher(path).replaceAll("_email_");
    return path;
  }


}
