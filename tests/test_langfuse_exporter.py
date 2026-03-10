from __future__ import annotations

from datetime import UTC, datetime

from uk_resell_adk.langfuse_exporter import _EventRow, _LinkRow, _SpanRow, _build_otlp_payload


def test_build_otlp_payload_groups_spans_by_resource() -> None:
    now = datetime(2026, 3, 8, 20, 0, tzinfo=UTC)
    span = _SpanRow(
        trace_id="00112233445566778899aabbccddeeff",
        span_id="1111222233334444",
        parent_span_id=None,
        span_name="run_local_dry_run",
        service_name="uk-resell-adk",
        environment="local",
        kind="2",
        start_time=now,
        end_time=now,
        status_code="1",
        status_message="OK",
        attributes={"http.status_code": 200},
        resource_attributes={"service.name": "uk-resell-adk"},
        ingestion_time=now,
    )
    event = _EventRow(
        trace_id=span.trace_id,
        span_id=span.span_id,
        event_time=now,
        event_name="db.query",
        attributes={"db.system": "postgresql"},
    )
    link = _LinkRow(
        trace_id=span.trace_id,
        span_id=span.span_id,
        linked_trace_id="ffeeddccbbaa99887766554433221100",
        linked_span_id="9999888877776666",
        attributes={"link.type": "follows_from"},
    )

    payload = _build_otlp_payload([span], [event], [link])
    assert "resourceSpans" in payload
    assert len(payload["resourceSpans"]) == 1
    exported_span = payload["resourceSpans"][0]["scopeSpans"][0]["spans"][0]
    assert exported_span["traceId"] == span.trace_id
    assert exported_span["spanId"] == span.span_id
    assert exported_span["events"][0]["name"] == "db.query"
    assert exported_span["links"][0]["spanId"] == "9999888877776666"
