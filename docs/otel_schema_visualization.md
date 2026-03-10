# OTel Postgres Schema Visualization

Open this file in VS Code and use **Markdown: Open Preview** to view the ER diagram.

```mermaid
erDiagram
    traces {
        text trace_id PK
        text root_span_id
        text service_name
        text environment
        timestamptz start_time
        timestamptz end_time
        float duration_ms
        text status_code
        text status_message
        jsonb resource_attributes
        timestamptz created_at
    }

    spans {
        text trace_id
        text span_id
        text parent_span_id
        text service_name
        text span_name
        text kind
        timestamptz start_time
        timestamptz end_time
        float duration_ms
        text status_code
        text status_message
        jsonb attributes
        int events_count
        int links_count
        timestamptz ingestion_time
    }

    span_events {
        text trace_id
        text span_id
        timestamptz event_time
        text event_name
        jsonb attributes
        timestamptz ingestion_time
    }

    span_links {
        text trace_id
        text span_id
        text linked_trace_id
        text linked_span_id
        jsonb attributes
        timestamptz ingestion_time
    }

    export_watermarks {
        text destination PK
        timestamptz watermark_time
        timestamptz updated_at
    }

    export_attempts {
        bigint id PK
        text destination
        text trace_id
        text span_id
        timestamptz attempted_at
        boolean success
        text status_code
        text error_message
    }

    traces ||--o{ spans : "trace_id"
    spans ||--o{ span_events : "trace_id + span_id"
    spans ||--o{ span_links : "trace_id + span_id"
```

## Partitioning

- `otel.spans` is range-partitioned by `start_time` (monthly partitions).
- `otel.span_events` is range-partitioned by `event_time` (monthly partitions).

Current partitions in your DB:

- `otel.spans_202503`, `otel.spans_202603`, `otel.spans_202604`
- `otel.span_events_202503`, `otel.span_events_202603`, `otel.span_events_202604`

## Key Indexes

- `traces`: PK on `trace_id`, plus service/status/time and JSONB GIN index.
- `spans`: indexes on `trace_id`, `span_id`, `parent_span_id`, `span_name`, service/status/time, JSONB GIN.
- `span_events`: `(trace_id, span_id, event_time)` and `(event_name, event_time)` plus JSONB GIN.
- `span_links`: `(trace_id, span_id)` and `(linked_trace_id, linked_span_id)`.

## Dedupe Constraints

- `spans`: unique `(trace_id, span_id, start_time)`
- `span_events`: unique `(trace_id, span_id, event_time, event_name)`
- `span_links`: unique `(trace_id, span_id, linked_trace_id, linked_span_id)`
