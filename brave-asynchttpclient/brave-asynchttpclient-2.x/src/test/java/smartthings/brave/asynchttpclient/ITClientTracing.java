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
package smartthings.brave.asynchttpclient;

import brave.http.ITHttpClient;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.SocketPolicy;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.AsyncHttpClientConfig;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.junit.After;
import org.junit.Test;
import zipkin.Constants;

import static org.assertj.core.api.Assertions.assertThat;

public class ITClientTracing extends ITHttpClient<AsyncHttpClient> {

  private int port;

  @Override protected AsyncHttpClient newClient(int port) {
    this.port = port;
    AsyncHttpClientConfig config = ClientTracing
      .instrument(new DefaultAsyncHttpClientConfig.Builder(), httpTracing)
      .setFollowRedirect(true)
      .setMaxRequestRetry(1)
      .setRequestTimeout(100)
      .build();

    return new DefaultAsyncHttpClient(config);
  }

  @After public void cleanup() {
    spans.clear();
  }

  @Override protected void closeClient(AsyncHttpClient asyncHttpClient) throws IOException {
    asyncHttpClient.close();
  }

  @Override protected void get(AsyncHttpClient asyncHttpClient, String path) throws Exception {
    asyncHttpClient.prepareGet(String.format("http://127.0.0.1:%d%s", port, path))
      .execute()
      .get(1, TimeUnit.SECONDS);
  }

  @Override protected void post(AsyncHttpClient asyncHttpClient, String path, String body)
    throws Exception {
    asyncHttpClient.preparePost(String.format("http://127.0.0.1:%d%s", port, path)).setBody(body)
      .execute()
      .get(1, TimeUnit.SECONDS);
  }

  @Override protected void getAsync(AsyncHttpClient asyncHttpClient, String path) throws Exception {
    asyncHttpClient.prepareGet(String.format("http://127.0.0.1:%d%s", port, path)).execute();
  }

  @Test public void addsErrorTagOnTransportException() throws Exception {
    server.enqueue(new MockResponse().setSocketPolicy(SocketPolicy.DISCONNECT_AT_START));

    try {
      get(client, "/foo");
    } catch (Exception e) {
      // ok, but the span should include an error!
    }

    assertThat(spans)
      .flatExtracting(s -> s.binaryAnnotations)
      .extracting(b -> b.key)
      .contains(Constants.ERROR);
  }

}
