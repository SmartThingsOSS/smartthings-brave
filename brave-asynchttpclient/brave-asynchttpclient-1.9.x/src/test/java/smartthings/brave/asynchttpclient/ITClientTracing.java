/**
 * Copyright 2016-2018 SmartThings
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

import brave.http.ITHttpClient;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.AsyncHttpClientConfig;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class ITClientTracing extends ITHttpClient<AsyncHttpClient> {

  private int port;

  @Override protected AsyncHttpClient newClient(int port) {
    this.port = port;

    AsyncHttpClientConfig config = ClientTracing
      .instrument(new AsyncHttpClientConfig.Builder(), httpTracing)
      .setFollowRedirect(true)
      .setMaxRequestRetry(1)
      .setRequestTimeout(100)
      .build();

    return new AsyncHttpClient(config);
  }

  @Override protected void closeClient(AsyncHttpClient client) throws IOException {
    client.close();
  }

  @Override protected void get(AsyncHttpClient client, String pathIncludingQuery) throws Exception {
    client.prepareGet("http://127.0.0.1:" + port + pathIncludingQuery)
      .execute()
      .get(1000, TimeUnit.MILLISECONDS);
  }

  @Override protected void post(AsyncHttpClient client, String pathIncludingQuery, String body)
    throws Exception {
    client.preparePost("http://127.0.0.1:" + port + pathIncludingQuery)
      .setBody(body)
      .execute()
      .get(1000, TimeUnit.MILLISECONDS);
  }
}
