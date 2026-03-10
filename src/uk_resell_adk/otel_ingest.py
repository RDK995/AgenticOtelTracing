from __future__ import annotations

import argparse
import base64
import binascii
import json
import logging
import os
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, cast

import psycopg
from psycopg import sql

try:
    from google.protobuf.json_format import MessageToDict
    from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest
except Exception:
    MessageToDict = None
    ExportTraceServiceRequest = None


LOGGER = logging.getLogger(__name__)


def _parse_any_value(node: dict[str, Any]) -> Any:
    if "stringValue" in node:
        return node["stringValue"]
    if "boolValue" in node:
        return bool(node["boolValue"])
    if "intValue" in node:
        try:
            return int(node["intValue"])
        except Exception:
            return str(node["intValue"])
    if "doubleValue" in node:
        try:
            return float(node["doubleValue"])
        except Exception:
            return str(node["doubleValue"])
    if "arrayValue" in node:
        values = node.get("arrayValue", {}).get("values", [])
        return [_parse_any_value(v) for v in values]
    if "kvlistValue" in node:
        values = node.get("kvlistValue", {}).get("values", [])
        return {v.get("key"): _parse_any_value(v.get("value", {})) for v in values if v.get("key")}
    if "bytesValue" in node:
        raw = node["bytesValue"]
        try:
            return base64.b64decode(raw).hex()
        except Exception:
            return raw
    return None


def _parse_attributes(entries: Iterable[dict[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for entry in entries:
        key = entry.get("key")
        value = entry.get("value")
        if not key or not isinstance(value, dict):
            continue
        result[str(key)] = _parse_any_value(value)
    return result


def _parse_unix_nanos(raw: Any) -> tuple[int | None, datetime | None]:
    if raw is None:
        return None, None
    try:
        nanos = int(raw)
    except Exception:
        return None, None
    return nanos, datetime.fromtimestamp(nanos / 1_000_000_000, tz=UTC)


def _normalize_span_or_trace_id(raw: Any, expected_num_bytes: int) -> str:
    text = str(raw or "").strip()
    if not text:
        return ""

    if len(text) == expected_num_bytes * 2:
        try:
            int(text, 16)
            return text.lower()
        except ValueError:
            pass

    try:
        decoded = base64.b64decode(text, validate=True)
        if len(decoded) == expected_num_bytes:
            return decoded.hex()
    except (binascii.Error, ValueError):
        pass

    return text


@dataclass(frozen=True)
class _TraceRow:
    trace_id: str
    root_span_id: str | None
    trace_name: str | None
    user_id: str | None
    session_id: str | None
    trace_input: str | None
    trace_output: str | None
    service_name: str
    environment: str | None
    start_time: datetime
    end_time: datetime
    duration_ms: float
    status_code: str | None
    status_message: str | None
    resource_attributes: dict[str, Any]


@dataclass(frozen=True)
class _SpanRow:
    trace_id: str
    span_id: str
    parent_span_id: str | None
    service_name: str
    span_name: str
    scope_name: str | None
    scope_version: str | None
    scope_attributes: dict[str, Any]
    kind: str | None
    start_time: datetime
    end_time: datetime | None
    duration_ms: float | None
    status_code: str | None
    status_message: str | None
    attributes: dict[str, Any]
    events_count: int
    links_count: int


@dataclass(frozen=True)
class _EventRow:
    trace_id: str
    span_id: str
    event_time: datetime
    event_name: str
    attributes: dict[str, Any]


@dataclass(frozen=True)
class _LinkRow:
    trace_id: str
    span_id: str
    linked_trace_id: str
    linked_span_id: str
    attributes: dict[str, Any]


def _to_optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _trace_identity_from_span_attributes(attributes: dict[str, Any]) -> tuple[str | None, str | None, str | None, str | None, str | None]:
    trace_name = _to_optional_text(attributes.get("langfuse.trace.name"))
    user_id = _to_optional_text(attributes.get("langfuse.user.id")) or _to_optional_text(attributes.get("user.id"))
    session_id = _to_optional_text(attributes.get("langfuse.session.id")) or _to_optional_text(attributes.get("session.id"))
    trace_input = _to_optional_text(attributes.get("langfuse.trace.input"))
    trace_output = _to_optional_text(attributes.get("langfuse.trace.output"))
    return trace_name, user_id, session_id, trace_input, trace_output


def _extract_rows(payload: dict[str, Any]) -> tuple[list[_TraceRow], list[_SpanRow], list[_EventRow], list[_LinkRow]]:
    traces: dict[str, _TraceRow] = {}
    spans: list[_SpanRow] = []
    events: list[_EventRow] = []
    links: list[_LinkRow] = []

    resource_spans = payload.get("resourceSpans", [])
    for resource_span in resource_spans:
        resource_attributes = _parse_attributes(resource_span.get("resource", {}).get("attributes", []))
        service_name = str(resource_attributes.get("service.name") or "unknown-service")
        environment = resource_attributes.get("deployment.environment")
        if environment is not None:
            environment = str(environment)

        scope_spans = resource_span.get("scopeSpans", []) or resource_span.get("instrumentationLibrarySpans", [])
        for scope_span in scope_spans:
            scope = scope_span.get("scope", {}) if isinstance(scope_span.get("scope"), dict) else {}
            if not scope:
                scope = (
                    scope_span.get("instrumentationLibrary", {})
                    if isinstance(scope_span.get("instrumentationLibrary"), dict)
                    else {}
                )
            scope_name = _to_optional_text(scope.get("name"))
            scope_version = _to_optional_text(scope.get("version"))
            scope_attributes = _parse_attributes(scope.get("attributes", []))
            for span in scope_span.get("spans", []):
                trace_id = _normalize_span_or_trace_id(span.get("traceId"), expected_num_bytes=16)
                span_id = _normalize_span_or_trace_id(span.get("spanId"), expected_num_bytes=8)
                if not trace_id or not span_id:
                    continue

                parent_span_id = _normalize_span_or_trace_id(span.get("parentSpanId"), expected_num_bytes=8) or None
                span_name = str(span.get("name") or "unnamed-span")
                kind = str(span.get("kind")) if span.get("kind") is not None else None
                span_attributes = _parse_attributes(span.get("attributes", []))
                status = span.get("status", {}) if isinstance(span.get("status"), dict) else {}
                status_code = str(status.get("code")) if status.get("code") is not None else None
                status_message = str(status.get("message")) if status.get("message") is not None else None

                start_nanos, start_dt = _parse_unix_nanos(span.get("startTimeUnixNano"))
                end_nanos, end_dt = _parse_unix_nanos(span.get("endTimeUnixNano"))
                if start_dt is None:
                    continue
                duration_ms: float | None = None
                if start_nanos is not None and end_nanos is not None and end_nanos >= start_nanos:
                    duration_ms = (end_nanos - start_nanos) / 1_000_000.0

                spans.append(
                    _SpanRow(
                        trace_id=trace_id,
                        span_id=span_id,
                        parent_span_id=parent_span_id,
                        service_name=service_name,
                        span_name=span_name,
                        scope_name=scope_name,
                        scope_version=scope_version,
                        scope_attributes=scope_attributes,
                        kind=kind,
                        start_time=start_dt,
                        end_time=end_dt,
                        duration_ms=duration_ms,
                        status_code=status_code,
                        status_message=status_message,
                        attributes=span_attributes,
                        events_count=len(span.get("events", [])),
                        links_count=len(span.get("links", [])),
                    )
                )

                existing_trace = traces.get(trace_id)
                trace_name, user_id, session_id, trace_input, trace_output = _trace_identity_from_span_attributes(span_attributes)
                if existing_trace is None:
                    root_span_id = span_id if not parent_span_id else None
                    traces[trace_id] = _TraceRow(
                        trace_id=trace_id,
                        root_span_id=root_span_id,
                        trace_name=trace_name if not parent_span_id else None,
                        user_id=user_id if not parent_span_id else None,
                        session_id=session_id if not parent_span_id else None,
                        trace_input=trace_input if not parent_span_id else None,
                        trace_output=trace_output if not parent_span_id else None,
                        service_name=service_name,
                        environment=environment,
                        start_time=start_dt,
                        end_time=end_dt or start_dt,
                        duration_ms=duration_ms or 0.0,
                        status_code=status_code,
                        status_message=status_message,
                        resource_attributes=resource_attributes,
                    )
                else:
                    trace_start = min(existing_trace.start_time, start_dt)
                    trace_end = max(existing_trace.end_time, end_dt or start_dt)
                    trace_duration_ms = (trace_end - trace_start).total_seconds() * 1000.0
                    root_span_id = existing_trace.root_span_id
                    if root_span_id is None and not parent_span_id:
                        root_span_id = span_id
                    traces[trace_id] = _TraceRow(
                        trace_id=trace_id,
                        root_span_id=root_span_id,
                        trace_name=existing_trace.trace_name or (trace_name if not parent_span_id else None),
                        user_id=existing_trace.user_id or (user_id if not parent_span_id else None),
                        session_id=existing_trace.session_id or (session_id if not parent_span_id else None),
                        trace_input=existing_trace.trace_input or (trace_input if not parent_span_id else None),
                        trace_output=existing_trace.trace_output or (trace_output if not parent_span_id else None),
                        service_name=existing_trace.service_name,
                        environment=existing_trace.environment,
                        start_time=trace_start,
                        end_time=trace_end,
                        duration_ms=trace_duration_ms,
                        status_code=existing_trace.status_code or status_code,
                        status_message=existing_trace.status_message or status_message,
                        resource_attributes=existing_trace.resource_attributes,
                    )

                for event in span.get("events", []):
                    _, event_dt = _parse_unix_nanos(event.get("timeUnixNano"))
                    if event_dt is None:
                        event_dt = start_dt
                    events.append(
                        _EventRow(
                            trace_id=trace_id,
                            span_id=span_id,
                            event_time=event_dt,
                            event_name=str(event.get("name") or "event"),
                            attributes=_parse_attributes(event.get("attributes", [])),
                        )
                    )

                for link in span.get("links", []):
                    linked_trace_id = _normalize_span_or_trace_id(link.get("traceId"), expected_num_bytes=16)
                    linked_span_id = _normalize_span_or_trace_id(link.get("spanId"), expected_num_bytes=8)
                    if not linked_trace_id or not linked_span_id:
                        continue
                    links.append(
                        _LinkRow(
                            trace_id=trace_id,
                            span_id=span_id,
                            linked_trace_id=linked_trace_id,
                            linked_span_id=linked_span_id,
                            attributes=_parse_attributes(link.get("attributes", [])),
                        )
                    )

    return list(traces.values()), spans, events, links


def _ensure_month_partition(conn: psycopg.Connection[Any], parent_table: str, month_start: datetime) -> None:
    month_start = month_start.astimezone(UTC).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if month_start.month == 12:
        next_month = month_start.replace(year=month_start.year + 1, month=1)
    else:
        next_month = month_start.replace(month=month_start.month + 1)
    partition_name = f"{parent_table}_{month_start.strftime('%Y%m')}"
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS otel.{partition}
                PARTITION OF otel.{parent}
                FOR VALUES FROM ({start}) TO ({end})
                """
            ).format(
                partition=sql.Identifier(partition_name),
                parent=sql.Identifier(parent_table),
                start=sql.Literal(month_start),
                end=sql.Literal(next_month),
            )
        )


def _ensure_required_partitions(conn: psycopg.Connection[Any], spans: list[_SpanRow], events: list[_EventRow]) -> None:
    span_months = {
        span.start_time.astimezone(UTC).replace(day=1, hour=0, minute=0, second=0, microsecond=0) for span in spans
    }
    event_months = {
        event.event_time.astimezone(UTC).replace(day=1, hour=0, minute=0, second=0, microsecond=0) for event in events
    }
    for month in sorted(span_months):
        _ensure_month_partition(conn, "spans", month)
    for month in sorted(event_months):
        _ensure_month_partition(conn, "span_events", month)


def _persist_rows(
    conn: psycopg.Connection[Any],
    traces: list[_TraceRow],
    spans: list[_SpanRow],
    events: list[_EventRow],
    links: list[_LinkRow],
) -> None:
    with conn.cursor() as cur:
        cur.execute("ALTER TABLE otel.traces ADD COLUMN IF NOT EXISTS trace_name TEXT")
        cur.execute("ALTER TABLE otel.traces ADD COLUMN IF NOT EXISTS user_id TEXT")
        cur.execute("ALTER TABLE otel.traces ADD COLUMN IF NOT EXISTS session_id TEXT")
        cur.execute("ALTER TABLE otel.traces ADD COLUMN IF NOT EXISTS trace_input TEXT")
        cur.execute("ALTER TABLE otel.traces ADD COLUMN IF NOT EXISTS trace_output TEXT")
        cur.execute("ALTER TABLE otel.spans ADD COLUMN IF NOT EXISTS scope_name TEXT")
        cur.execute("ALTER TABLE otel.spans ADD COLUMN IF NOT EXISTS scope_version TEXT")
        cur.execute("ALTER TABLE otel.spans ADD COLUMN IF NOT EXISTS scope_attributes JSONB NOT NULL DEFAULT '{}'::jsonb")

    _ensure_required_partitions(conn, spans=spans, events=events)

    with conn.cursor() as cur:
        for trace in traces:
            cur.execute(
                """
                INSERT INTO otel.traces (
                    trace_id,
                    root_span_id,
                    trace_name,
                    user_id,
                    session_id,
                    trace_input,
                    trace_output,
                    service_name,
                    environment,
                    start_time,
                    end_time,
                    duration_ms,
                    status_code,
                    status_message,
                    resource_attributes
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (trace_id) DO UPDATE SET
                    root_span_id = COALESCE(otel.traces.root_span_id, EXCLUDED.root_span_id),
                    trace_name = COALESCE(otel.traces.trace_name, EXCLUDED.trace_name),
                    user_id = COALESCE(otel.traces.user_id, EXCLUDED.user_id),
                    session_id = COALESCE(otel.traces.session_id, EXCLUDED.session_id),
                    trace_input = COALESCE(otel.traces.trace_input, EXCLUDED.trace_input),
                    trace_output = COALESCE(otel.traces.trace_output, EXCLUDED.trace_output),
                    start_time = LEAST(otel.traces.start_time, EXCLUDED.start_time),
                    end_time = GREATEST(otel.traces.end_time, EXCLUDED.end_time),
                    duration_ms = GREATEST(otel.traces.duration_ms, EXCLUDED.duration_ms),
                    status_code = COALESCE(otel.traces.status_code, EXCLUDED.status_code),
                    status_message = COALESCE(otel.traces.status_message, EXCLUDED.status_message),
                    resource_attributes = otel.traces.resource_attributes || EXCLUDED.resource_attributes
                """,
                (
                    trace.trace_id,
                    trace.root_span_id,
                    trace.trace_name,
                    trace.user_id,
                    trace.session_id,
                    trace.trace_input,
                    trace.trace_output,
                    trace.service_name,
                    trace.environment,
                    trace.start_time,
                    trace.end_time,
                    trace.duration_ms,
                    trace.status_code,
                    trace.status_message,
                    json.dumps(trace.resource_attributes),
                ),
            )

        cur.executemany(
            """
            INSERT INTO otel.spans (
                trace_id,
                span_id,
                parent_span_id,
                service_name,
                span_name,
                scope_name,
                scope_version,
                scope_attributes,
                kind,
                start_time,
                end_time,
                duration_ms,
                status_code,
                status_message,
                attributes,
                events_count,
                links_count
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            [
                (
                    span.trace_id,
                    span.span_id,
                    span.parent_span_id,
                    span.service_name,
                    span.span_name,
                    span.scope_name,
                    span.scope_version,
                    json.dumps(span.scope_attributes),
                    span.kind,
                    span.start_time,
                    span.end_time,
                    span.duration_ms,
                    span.status_code,
                    span.status_message,
                    json.dumps(span.attributes),
                    span.events_count,
                    span.links_count,
                )
                for span in spans
            ],
        )

        cur.executemany(
            """
            INSERT INTO otel.span_events (
                trace_id,
                span_id,
                event_time,
                event_name,
                attributes
            ) VALUES (%s, %s, %s, %s, %s::jsonb)
            ON CONFLICT DO NOTHING
            """,
            [
                (
                    event.trace_id,
                    event.span_id,
                    event.event_time,
                    event.event_name,
                    json.dumps(event.attributes),
                )
                for event in events
            ],
        )

        cur.executemany(
            """
            INSERT INTO otel.span_links (
                trace_id,
                span_id,
                linked_trace_id,
                linked_span_id,
                attributes
            ) VALUES (%s, %s, %s, %s, %s::jsonb)
            ON CONFLICT DO NOTHING
            """,
            [
                (
                    link.trace_id,
                    link.span_id,
                    link.linked_trace_id,
                    link.linked_span_id,
                    json.dumps(link.attributes),
                )
                for link in links
            ],
        )


class _ServerState:
    def __init__(self, db_dsn: str, traces_path: str) -> None:
        self.db_dsn = db_dsn
        self.traces_path = traces_path


def _parse_otlp_payload(body: bytes, content_type: str) -> dict[str, Any]:
    normalized_content_type = content_type.lower().split(";")[0].strip()
    if normalized_content_type in {"application/json", "application/x-ndjson"}:
        return cast(dict[str, Any], json.loads(body.decode("utf-8")))

    if normalized_content_type in {"application/x-protobuf", "application/protobuf"}:
        if ExportTraceServiceRequest is None or MessageToDict is None:
            raise ValueError("protobuf payload support is unavailable")
        request = ExportTraceServiceRequest()
        request.ParseFromString(body)
        payload = MessageToDict(
            request,
            preserving_proto_field_name=False,
            use_integers_for_enums=True,
        )
        if not isinstance(payload, dict):
            raise ValueError("invalid protobuf payload")
        return cast(dict[str, Any], payload)

    raise ValueError(f"unsupported content-type: {normalized_content_type or 'unknown'}")


def _build_handler(state: _ServerState) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        server_version = "otel-ingest/1.0"

        def do_POST(self) -> None:  # noqa: N802
            if self.path.rstrip("/") != state.traces_path.rstrip("/"):
                self.send_error(HTTPStatus.NOT_FOUND, "unknown endpoint")
                return

            content_length = int(self.headers.get("Content-Length", "0"))
            if content_length <= 0:
                self.send_error(HTTPStatus.BAD_REQUEST, "empty request body")
                return
            body = self.rfile.read(content_length)
            content_type = str(self.headers.get("Content-Type", "application/json"))
            try:
                payload = _parse_otlp_payload(body, content_type)
            except Exception:
                self.send_error(HTTPStatus.BAD_REQUEST, "expected OTLP JSON or protobuf body")
                return

            traces, spans, events, links = _extract_rows(payload)
            if not spans:
                self.send_response(HTTPStatus.ACCEPTED)
                self.end_headers()
                return

            try:
                with psycopg.connect(state.db_dsn, autocommit=False) as conn:
                    _persist_rows(conn, traces=traces, spans=spans, events=events, links=links)
                    conn.commit()
            except Exception as exc:
                LOGGER.exception("Failed to persist OTLP spans: %s", exc)
                self.send_error(HTTPStatus.INTERNAL_SERVER_ERROR, "failed to persist spans")
                return

            response_body = json.dumps(
                {
                    "accepted_trace_count": len(traces),
                    "accepted_span_count": len(spans),
                    "accepted_event_count": len(events),
                    "accepted_link_count": len(links),
                }
            ).encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(response_body)))
            self.end_headers()
            self.wfile.write(response_body)

        def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
            LOGGER.info("%s - %s", self.client_address[0], format % args)

    return Handler


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Receive OTLP JSON traces and store them in Postgres.")
    parser.add_argument("--host", default=os.getenv("OTEL_INGEST_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.getenv("OTEL_INGEST_PORT", "4318")))
    parser.add_argument("--path", default=os.getenv("OTEL_INGEST_TRACES_PATH", "/v1/traces"))
    parser.add_argument(
        "--db-dsn",
        default=os.getenv(
            "OTEL_POSTGRES_DSN",
            "postgresql://otel_user:otel_password@localhost:5432/otel_observability",
        ),
    )
    parser.add_argument("--log-level", default=os.getenv("OTEL_INGEST_LOG_LEVEL", "INFO"))
    return parser


def main() -> None:
    args = _build_arg_parser().parse_args()
    logging.basicConfig(level=getattr(logging, str(args.log_level).upper(), logging.INFO))

    state = _ServerState(db_dsn=args.db_dsn, traces_path=args.path)
    handler = _build_handler(state)

    with ThreadingHTTPServer((args.host, args.port), handler) as httpd:
        LOGGER.info("OTLP Postgres ingest listening on http://%s:%s%s", args.host, args.port, args.path)
        httpd.serve_forever()


if __name__ == "__main__":
    main()
