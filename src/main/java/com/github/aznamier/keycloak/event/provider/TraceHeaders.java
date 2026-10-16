package com.github.aznamier.keycloak.event.provider;

import java.util.Map;

import org.jboss.logging.Logger;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;

/**
 * Adds the current OpenTelemetry trace (Keycloak's span for the request, when tracing is enabled) to a message's
 * headers, so consumers can continue the trace: {@code traceparent} in the W3C format, and the plain {@code traceId}
 * for searching by hand. Nothing is added when there is no current trace.
 */
final class TraceHeaders {

	private static final Logger log = Logger.getLogger(TraceHeaders.class);

	static final String TRACEPARENT = "traceparent";
	static final String TRACE_ID = "traceId";

	private static volatile boolean unavailable;

	private TraceHeaders() {
	}

	static void addTo(Map<String, Object> headers) {
		if (unavailable) {
			return;
		}
		try {
			SpanContext context = Span.current().getSpanContext();
			if (!context.isValid()) {
				return;
			}
			headers.put(TRACEPARENT, "00-" + context.getTraceId() + "-" + context.getSpanId() + "-" + context.getTraceFlags().asHex());
			headers.put(TRACE_ID, context.getTraceId());
		} catch (NoClassDefFoundError e) {
			unavailable = true;
			log.warn("keycloak-to-rabbitmq OpenTelemetry is not available, so messages are sent without trace headers");
		}
	}
}
