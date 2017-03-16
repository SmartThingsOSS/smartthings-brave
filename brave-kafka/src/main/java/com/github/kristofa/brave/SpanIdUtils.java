
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
package com.github.kristofa.brave;

import brave.Span;
import brave.Tracer;
import brave.propagation.TraceContext;
import com.sun.istack.internal.Nullable;

/**
 * Contains some util functions to get around some access modifiers
 */
public class SpanIdUtils {

  public static SpanId traceContextToSpanId(TraceContext traceContext) {
    return new SpanId.Builder()
      .traceIdHigh(traceContext.traceIdHigh())
      .traceId(traceContext.traceId())
      .parentId(traceContext.parentId())
      .spanId(traceContext.spanId())
      .sampled(traceContext.sampled())
      .debug(traceContext.debug())
      .shared(traceContext.shared())
      .build();
  }

  public static TraceContext spanIdToTraceContext(SpanId spanId) {
    return TraceContext.newBuilder()
      .traceIdHigh(spanId.traceIdHigh)
      .traceId(spanId.traceId)
      .parentId(spanId.nullableParentId())
      .sampled(spanId.sampled())
      .spanId(spanId.spanId)
      .shared(spanId.shared)
      .build();
  }

  public static Span getNextSpan(@Nullable ClientTracer clientTracer, Tracer tracer) {
    if (clientTracer != null && clientTracer.maybeParent() != null) {
      SpanId parentSpanId = clientTracer.maybeParent();
      return tracer.newChild(SpanIdUtils.spanIdToTraceContext(parentSpanId));
    } else {
      return tracer.newTrace();
    }
  }
}
