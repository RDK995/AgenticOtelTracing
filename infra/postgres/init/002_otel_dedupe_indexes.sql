CREATE UNIQUE INDEX IF NOT EXISTS uq_spans_trace_span_start
ON otel.spans (trace_id, span_id, start_time);

CREATE UNIQUE INDEX IF NOT EXISTS uq_span_events_trace_span_time_name
ON otel.span_events (trace_id, span_id, event_time, event_name);

CREATE UNIQUE INDEX IF NOT EXISTS uq_span_links_trace_span_linked
ON otel.span_links (trace_id, span_id, linked_trace_id, linked_span_id);
