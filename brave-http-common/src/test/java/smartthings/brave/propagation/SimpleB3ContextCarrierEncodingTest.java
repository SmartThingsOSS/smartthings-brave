/**
 * Copyright 2016-2019 SmartThings
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
package smartthings.brave.propagation;

import brave.internal.HexCodec;
import org.junit.Ignore;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class SimpleB3ContextCarrierEncodingTest {

  @Test
  public void encodesRequiredFields() {
    SimpleB3ContextCarrier carrier = SimpleB3ContextCarrier.newBuilder().build();
    SimpleB3ContextCarrier.Setter setter = new SimpleB3ContextCarrier.Setter();
    setter.put(carrier, SimpleB3ContextCarrier.TRACE_ID_NAME, HexCodec.toLowerHex(1234));
    setter.put(carrier, SimpleB3ContextCarrier.SPAN_ID_NAME, HexCodec.toLowerHex(9012));

    String encoded = SimpleB3ContextCarrier.Encoding.encode(carrier);

    assertThat(encoded).isEqualTo("n00000000000004d20000000000002334nn");
  }

  @Test
  public void encodesExtraFields() {
    SimpleB3ContextCarrier carrier = SimpleB3ContextCarrier.newBuilder().build();
    SimpleB3ContextCarrier.Setter setter = new SimpleB3ContextCarrier.Setter();
    setter.put(carrier, SimpleB3ContextCarrier.TRACE_ID_NAME, HexCodec.toLowerHex(1234));
    setter.put(carrier, SimpleB3ContextCarrier.PARENT_SPAN_ID_NAME, HexCodec.toLowerHex(5678));
    setter.put(carrier, SimpleB3ContextCarrier.SPAN_ID_NAME, HexCodec.toLowerHex(9012));
    setter.put(carrier, SimpleB3ContextCarrier.SAMPLED_NAME, "1");

    String encoded = SimpleB3ContextCarrier.Encoding.encode(carrier);

    assertThat(encoded).isEqualTo("n00000000000004d20000000000002334000000000000162e0000000000000002");

    assertThat(carrier.isDebug()).isFalse();
    assertThat(carrier.isSampled()).isTrue();
    assertThat(carrier.isRedirect()).isFalse();
  }

  @Test
  public void decodesRequiredFields() {
    String encoded = "n00000000000004d20000000000002334nn";

    SimpleB3ContextCarrier carrier = SimpleB3ContextCarrier.Encoding.decode(encoded);
    SimpleB3ContextCarrier.Getter getter = new SimpleB3ContextCarrier.Getter();

    assertThat(getter.get(carrier, SimpleB3ContextCarrier.TRACE_ID_NAME)).isEqualTo("00000000000004d2");
    assertThat(getter.get(carrier, SimpleB3ContextCarrier.SPAN_ID_NAME)).isEqualTo("0000000000002334");
    assertThat(getter.get(carrier, SimpleB3ContextCarrier.SAMPLED_NAME)).isEqualTo("0");

    assertThat(carrier.isDebug()).isFalse();
    assertThat(carrier.isSampled()).isFalse();
    assertThat(carrier.isRedirect()).isFalse();
  }

  @Test
  public void decodesExtraFields() {
    String encoded = "n00000000000004d20000000000002334000000000000162e0000000000000002";

    SimpleB3ContextCarrier carrier = SimpleB3ContextCarrier.Encoding.decode(encoded);
    SimpleB3ContextCarrier.Getter getter = new SimpleB3ContextCarrier.Getter();

    assertThat(getter.get(carrier, SimpleB3ContextCarrier.TRACE_ID_NAME)).isEqualTo("00000000000004d2");
    assertThat(getter.get(carrier, SimpleB3ContextCarrier.PARENT_SPAN_ID_NAME)).isEqualTo("000000000000162e");
    assertThat(getter.get(carrier, SimpleB3ContextCarrier.SPAN_ID_NAME)).isEqualTo("0000000000002334");
    assertThat(getter.get(carrier, SimpleB3ContextCarrier.SAMPLED_NAME)).isEqualTo("1");
    assertThat(getter.get(carrier, SimpleB3ContextCarrier.FLAGS_NAME)).isEqualTo("0000000000000002");

    assertThat(carrier.isDebug()).isFalse();
    assertThat(carrier.isSampled()).isTrue();
    assertThat(carrier.isRedirect()).isFalse();
  }

  @Test
  public void builderShouldSetFlags() {
    SimpleB3ContextCarrier carrier = SimpleB3ContextCarrier.newBuilder()
      .setDebug(true)
      .setSampled(true)
      .setRedirect(true)
      .setComplete(true)
      .build();

    SimpleB3ContextCarrier.Setter setter = new SimpleB3ContextCarrier.Setter();
    setter.put(carrier, SimpleB3ContextCarrier.TRACE_ID_NAME, HexCodec.toLowerHex(1234));
    setter.put(carrier, SimpleB3ContextCarrier.SPAN_ID_NAME, HexCodec.toLowerHex(9012));

    String encoded = SimpleB3ContextCarrier.Encoding.encode(carrier);

    assertThat(encoded).isEqualTo("n00000000000004d20000000000002334n000000000000000f");
    assertThat(carrier.isDebug()).isTrue();
    assertThat(carrier.isSampled()).isTrue();
    assertThat(carrier.isRedirect()).isTrue();
    assertThat(carrier.isComplete()).isTrue();
  }

  @Test
  public void builderShouldCopyFrom() {
    SimpleB3ContextCarrier carrier = SimpleB3ContextCarrier.newBuilder()
      .setDebug(true)
      .setSampled(true)
      .setRedirect(true)
      .build();

    SimpleB3ContextCarrier.Setter setter = new SimpleB3ContextCarrier.Setter();
    setter.put(carrier, SimpleB3ContextCarrier.TRACE_ID_NAME, HexCodec.toLowerHex(1234));
    setter.put(carrier, SimpleB3ContextCarrier.SPAN_ID_NAME, HexCodec.toLowerHex(9012));
    setter.put(carrier, SimpleB3ContextCarrier.PARENT_SPAN_ID_NAME, HexCodec.toLowerHex(5678));

    SimpleB3ContextCarrier carrier2 = SimpleB3ContextCarrier.newBuilderFrom(carrier).build();

    assertThat(carrier2).isEqualTo(carrier);
  }

  @Ignore
  @Test
  public void injectingFlagsShouldNotUnsetBuilderValues() {
    SimpleB3ContextCarrier carrier = SimpleB3ContextCarrier.newBuilder()
      .setDebug(false)
      .setSampled(true)
      .setRedirect(true)
      .build();

    SimpleB3ContextCarrier.Setter setter = new SimpleB3ContextCarrier.Setter();
    setter.put(carrier, SimpleB3ContextCarrier.TRACE_ID_NAME, HexCodec.toLowerHex(1234));
    setter.put(carrier, SimpleB3ContextCarrier.SPAN_ID_NAME, HexCodec.toLowerHex(9012));
    setter.put(carrier, SimpleB3ContextCarrier.FLAGS_NAME, "0000000000000000"); // TODO 0 is not a valid hex anymore

    String encoded = SimpleB3ContextCarrier.Encoding.encode(carrier);

    assertThat(encoded).isEqualTo("n00000000000004d20000000000002334n0000000000000006");
    assertThat(carrier.isDebug()).isFalse();
    assertThat(carrier.isSampled()).isTrue();
    assertThat(carrier.isRedirect()).isTrue();
  }

  @Ignore
  @Test
  public void injectingZeroFlagsShouldNotSetValues() {
    SimpleB3ContextCarrier carrier = SimpleB3ContextCarrier.newBuilder()
      .setDebug(false)
      .setSampled(false)
      .setRedirect(false)
      .build();

    SimpleB3ContextCarrier.Setter setter = new SimpleB3ContextCarrier.Setter();
    setter.put(carrier, SimpleB3ContextCarrier.TRACE_ID_NAME, HexCodec.toLowerHex(1234));
    setter.put(carrier, SimpleB3ContextCarrier.SPAN_ID_NAME, HexCodec.toLowerHex(9012));
    setter.put(carrier, SimpleB3ContextCarrier.FLAGS_NAME, "0000000000000000"); // TODO 0 is not a valid hex anymore

    String encoded = SimpleB3ContextCarrier.Encoding.encode(carrier);

    assertThat(encoded).isEqualTo("n00000000000004d20000000000002334n0000000000000000");
    assertThat(carrier.isDebug()).isFalse();
    assertThat(carrier.isSampled()).isFalse();
    assertThat(carrier.isRedirect()).isFalse();
  }



}
