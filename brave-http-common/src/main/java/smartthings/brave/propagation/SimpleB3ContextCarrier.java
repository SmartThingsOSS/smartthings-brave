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
package smartthings.brave.propagation;

import brave.internal.HexCodec;
import brave.propagation.B3Propagation;
import brave.propagation.Propagation;
import brave.propagation.TraceContext;

/**
 *
 */

public final class SimpleB3ContextCarrier {

  /**
   * 128 or 64-bit trace ID lower-hex encoded into 32 or 16 characters (required)
   */
  static final String TRACE_ID_NAME = "X-B3-TraceId";
  /**
   * 64-bit span ID lower-hex encoded into 16 characters (required)
   */
  static final String SPAN_ID_NAME = "X-B3-SpanId";
  /**
   * 64-bit parent span ID lower-hex encoded into 16 characters (absent on root span)
   */
  static final String PARENT_SPAN_ID_NAME = "X-B3-ParentSpanId";
  /**
   * "1" means report this span to the tracing system, "0" means do not. (absent means defer the
   * decision to the receiver of this header).
   */
  static final String SAMPLED_NAME = "X-B3-Sampled";
  /**
   * "1" implies sampled and is a request to override collection-tier sampling policy.
   */
  static final String FLAGS_NAME = "X-B3-Flags";

  public static final TraceContext.Injector<SimpleB3ContextCarrier> INJECTOR =
    B3Propagation.B3_STRING.injector(new Setter());

  public static final TraceContext.Extractor<SimpleB3ContextCarrier> EXTRACTOR =
    B3Propagation.B3_STRING.extractor(new Getter());


  private String traceIdHigh;
  private String traceId;
  private String parentId = null;
  private String spanId;
  private Long flags = null;

  public SimpleB3ContextCarrier() {}

  SimpleB3ContextCarrier(long traceIdHigh, long traceId, long spanId) {
    this.traceIdHigh = HexCodec.toLowerHex(traceIdHigh);
    this.traceId = HexCodec.toLowerHex(traceId);
    this.spanId = HexCodec.toLowerHex(spanId);
  }

  SimpleB3ContextCarrier(long traceIdHigh, long traceId, long spanId, long parentId, long flags) {
    this(traceIdHigh, traceId, spanId);
    this.parentId = HexCodec.toLowerHex(parentId);
    this.flags = flags;
  }

  private boolean isRedirect() {
    return flags != null && ((flags & (1<<2)) != 0);
  }

  private SimpleB3ContextCarrier setRedirect(boolean redirect) {
    if (flags == null) flags = 0L;

    if (redirect) {
      flags = (flags | (1<<2));
    } else {
      flags = (flags & ~(1<<2));
    }
    return this;
  }

  private boolean isDebug() {
    return flags != null && ((flags & (1<<0)) != 0);
  }

  private SimpleB3ContextCarrier setDebug(boolean debug) {
    if (flags == null) flags = 0L;

    if (debug) {
      flags = (flags | (1<<0));
    } else {
      flags = (flags & ~(1<<0));
    }
    return this;
  }

  private boolean isSampled() {
    return flags != null && ((flags & (1<<1)) != 0);
  }

  private SimpleB3ContextCarrier setSampled(boolean sampled) {
    if (flags == null) flags = 0L;

    if (sampled) {
      flags = (flags | (1<<1));
    } else {
      flags = (flags & ~(1<<1));
    }
    return this;
  }



  public static final class Encoding {

    private Encoding() {}

    public static String encode(SimpleB3ContextCarrier carrier) {
      StringBuilder sb = new StringBuilder();
      sb.append((carrier.traceIdHigh != null) ? carrier.traceIdHigh : "n");
      sb.append(carrier.traceId);
      sb.append(carrier.spanId);
      if (carrier.parentId != null) {
        sb.append(carrier.parentId);
      } else {
        sb.append("n");
      }
      if (carrier.flags != null) {
        sb.append(HexCodec.toLowerHex(carrier.flags));
      } else {
        sb.append("n");
      }
      return sb.toString();
    }

    public static SimpleB3ContextCarrier decode(String value) {
      if (value == null) throw new IllegalArgumentException("non null value is required");

      char[] chars = value.toCharArray();
      int i = 0;

      SimpleB3ContextCarrier c = new SimpleB3ContextCarrier();
      if (chars[i] == 'n') {
        c.traceIdHigh = null;
        i += 1;
      } else {
        c.traceIdHigh = new String(chars, i, 16);
        i += 16;
      }
      c.traceId = new String(chars, i, 16);
      i += 16;
      c.spanId = new String(chars, i, 16);
      i += 16;

      if (chars[i] == 'n') {
        c.parentId = null;
        i += 1;
      } else {
        c.parentId = new String(chars, i, 16);
        i += 16;
      }

      if (chars[i] == 'n') {
        c.flags = null;
      } else {
        c.flags = HexCodec.lowerHexToUnsignedLong(new String(chars, i, 16));
      }

      return c;
    }

  }

  public static final class Getter implements Propagation.Getter<SimpleB3ContextCarrier, String> {

    @Override public String get(SimpleB3ContextCarrier carrier, String key) {
      if (TRACE_ID_NAME.equals(key)) {
        return (carrier.traceIdHigh != null) ? carrier.traceIdHigh + carrier.traceId : carrier.traceId;
      } else if (SPAN_ID_NAME.equals(key)) {
        return carrier.spanId;
      } else if (PARENT_SPAN_ID_NAME.equals(key)) {
        if (carrier.parentId != null) {
          return carrier.parentId;
        } else {
          return null;
        }
      } else if (SAMPLED_NAME.equals(key)) {
        return carrier.isSampled() ? "1" : "0";
      } else if (FLAGS_NAME.equals(key)) {
        if (carrier.flags != null) {
          return HexCodec.toLowerHex(carrier.flags);
        } else {
          return null;
        }
      } else {
        return null;
      }
    }
  }

  public static final class Setter implements Propagation.Setter<SimpleB3ContextCarrier, String> {

    @Override public void put(SimpleB3ContextCarrier carrier, String key, String value) {
      if (TRACE_ID_NAME.equals(key)) {
        carrier.traceIdHigh = (value.length() == 32) ? value.substring(0, 15) : null;
        carrier.traceId = (value.length() == 32) ? value.substring(16) : value;
      } else if (SPAN_ID_NAME.equals(key)) {
        carrier.spanId = value;
      } else if (PARENT_SPAN_ID_NAME.equals(key) && value != null && !value.equals("")) {
        carrier.parentId = value;
      } else if (SAMPLED_NAME.equals(key) && value != null && !value.equals("")) {
        carrier.setSampled(value.equals("1") || value.equals("true"));
      } else if (FLAGS_NAME.equals(key) && value != null && !value.equals("")) {
        carrier.flags = HexCodec.lowerHexToUnsignedLong(value);
      }
    }

  }

}
