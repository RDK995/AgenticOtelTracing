CREATE SCHEMA IF NOT EXISTS otel;

CREATE TABLE IF NOT EXISTS otel.traces (
    trace_id TEXT PRIMARY KEY,
    root_span_id TEXT,
    trace_name TEXT,
    user_id TEXT,
    session_id TEXT,
    trace_input TEXT,
    trace_output TEXT,
    service_name TEXT NOT NULL,
    environment TEXT,
    start_time TIMESTAMPTZ NOT NULL,
    end_time TIMESTAMPTZ,
    duration_ms DOUBLE PRECISION,
    status_code TEXT,
    status_message TEXT,
    resource_attributes JSONB NOT NULL DEFAULT '{}'::JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS otel.spans (
    trace_id TEXT NOT NULL,
    span_id TEXT NOT NULL,
    parent_span_id TEXT,
    service_name TEXT NOT NULL,
    span_name TEXT NOT NULL,
    scope_name TEXT,
    scope_version TEXT,
    scope_attributes JSONB NOT NULL DEFAULT '{}'::JSONB,
    kind TEXT,
    start_time TIMESTAMPTZ NOT NULL,
    end_time TIMESTAMPTZ,
    duration_ms DOUBLE PRECISION,
    status_code TEXT,
    status_message TEXT,
    attributes JSONB NOT NULL DEFAULT '{}'::JSONB,
    events_count INTEGER NOT NULL DEFAULT 0,
    links_count INTEGER NOT NULL DEFAULT 0,
    ingestion_time TIMESTAMPTZ NOT NULL DEFAULT NOW()
) PARTITION BY RANGE (start_time);

CREATE TABLE IF NOT EXISTS otel.span_events (
    trace_id TEXT NOT NULL,
    span_id TEXT NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    event_name TEXT NOT NULL,
    attributes JSONB NOT NULL DEFAULT '{}'::JSONB,
    ingestion_time TIMESTAMPTZ NOT NULL DEFAULT NOW()
) PARTITION BY RANGE (event_time);

CREATE TABLE IF NOT EXISTS otel.span_links (
    trace_id TEXT NOT NULL,
    span_id TEXT NOT NULL,
    linked_trace_id TEXT NOT NULL,
    linked_span_id TEXT NOT NULL,
    attributes JSONB NOT NULL DEFAULT '{}'::JSONB,
    ingestion_time TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS otel.export_watermarks (
    destination TEXT PRIMARY KEY,
    watermark_time TIMESTAMPTZ NOT NULL DEFAULT '1970-01-01 00:00:00+00',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS otel.export_attempts (
    id BIGSERIAL PRIMARY KEY,
    destination TEXT NOT NULL,
    trace_id TEXT NOT NULL,
    span_id TEXT,
    attempted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    success BOOLEAN NOT NULL,
    status_code TEXT,
    error_message TEXT
);

CREATE INDEX IF NOT EXISTS idx_traces_service_start ON otel.traces (service_name, start_time DESC);
CREATE INDEX IF NOT EXISTS idx_traces_status_start ON otel.traces (status_code, start_time DESC);
CREATE INDEX IF NOT EXISTS idx_traces_resource_attributes_gin ON otel.traces USING GIN (resource_attributes);

CREATE INDEX IF NOT EXISTS idx_spans_trace_id ON otel.spans (trace_id);
CREATE INDEX IF NOT EXISTS idx_spans_span_id ON otel.spans (span_id);
CREATE INDEX IF NOT EXISTS idx_spans_parent_span_id ON otel.spans (parent_span_id);
CREATE INDEX IF NOT EXISTS idx_spans_service_start ON otel.spans (service_name, start_time DESC);
CREATE INDEX IF NOT EXISTS idx_spans_status_start ON otel.spans (status_code, start_time DESC);
CREATE INDEX IF NOT EXISTS idx_spans_name_start ON otel.spans (span_name, start_time DESC);
CREATE INDEX IF NOT EXISTS idx_spans_attributes_gin ON otel.spans USING GIN (attributes);

CREATE INDEX IF NOT EXISTS idx_span_events_trace_span_time ON otel.span_events (trace_id, span_id, event_time DESC);
CREATE INDEX IF NOT EXISTS idx_span_events_name_time ON otel.span_events (event_name, event_time DESC);
CREATE INDEX IF NOT EXISTS idx_span_events_attributes_gin ON otel.span_events USING GIN (attributes);

CREATE INDEX IF NOT EXISTS idx_span_links_trace_span ON otel.span_links (trace_id, span_id);
CREATE INDEX IF NOT EXISTS idx_span_links_linked_trace_span ON otel.span_links (linked_trace_id, linked_span_id);

DO $$
DECLARE
    month_start DATE := DATE_TRUNC('month', NOW())::DATE;
    next_month_start DATE := (DATE_TRUNC('month', NOW()) + INTERVAL '1 month')::DATE;
    month_after_next DATE := (DATE_TRUNC('month', NOW()) + INTERVAL '2 month')::DATE;
BEGIN
    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS otel.spans_%s PARTITION OF otel.spans FOR VALUES FROM (%L) TO (%L)',
        TO_CHAR(month_start, 'YYYYMM'),
        month_start,
        next_month_start
    );

    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS otel.spans_%s PARTITION OF otel.spans FOR VALUES FROM (%L) TO (%L)',
        TO_CHAR(next_month_start, 'YYYYMM'),
        next_month_start,
        month_after_next
    );

    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS otel.span_events_%s PARTITION OF otel.span_events FOR VALUES FROM (%L) TO (%L)',
        TO_CHAR(month_start, 'YYYYMM'),
        month_start,
        next_month_start
    );

    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS otel.span_events_%s PARTITION OF otel.span_events FOR VALUES FROM (%L) TO (%L)',
        TO_CHAR(next_month_start, 'YYYYMM'),
        next_month_start,
        month_after_next
    );
END $$;

INSERT INTO otel.export_watermarks (destination)
VALUES ('langfuse'), ('dynatrace'), ('cloud_trace')
ON CONFLICT (destination) DO NOTHING;
