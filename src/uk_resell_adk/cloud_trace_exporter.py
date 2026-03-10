from __future__ import annotations

import argparse
import base64
import json
import logging
import os
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import httpx
import psycopg
from google.auth.transport.requests import Request
from google.protobuf.json_format import ParseDict

try:
    import google.auth
except Exception:
    google = None

try:
    from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest
except Exception:
    ExportTraceServiceRequest = None


LOGGER = logging.getLogger(__name__)

_DESTINATION = "cloud_trace"


@dataclass(frozen=True)
class _SpanRow:
    trace_id: str
    span_id: str
    parent_span_id: str | None
    span_name: str
    service_name: str
    environment: str | None
    scope_name: str | None
    scope_version: str | None
    scope_attributes: dict[str, Any]
    kind: str | None
    start_time: datetime
    end_time: datetime | None
    status_code: str | None
    status_message: str | None
    attributes: dict[str, Any]
    trace_name: str | None
    user_id: str | None
    session_id: str | None
    trace_input: str | None
    trace_output: str | None
    resource_attributes: dict[str, Any]
    ingestion_time: datetime


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


def _to_unix_nanos(dt: datetime) -> str:
    aware = dt if dt.tzinfo is not None else dt.replace(tzinfo=UTC)
    return str(int(aware.timestamp() * 1_000_000_000))


def _hex_id_to_b64(raw: str, expected_num_bytes: int) -> str:
    value = str(raw or "").strip().lower()
    if not value:
        return ""
    try:
        decoded = bytes.fromhex(value)
    except ValueError:
        return ""
    if len(decoded) != expected_num_bytes:
        return ""
    return base64.b64encode(decoded).decode("ascii")


def _to_otel_value(value: Any) -> dict[str, Any]:
    if value is None:
        return {"stringValue": "null"}
    if isinstance(value, bool):
        return {"boolValue": value}
    if isinstance(value, int) and not isinstance(value, bool):
        return {"intValue": str(value)}
    if isinstance(value, float):
        return {"doubleValue": value}
    if isinstance(value, str):
        return {"stringValue": value}
    if isinstance(value, list):
        return {"arrayValue": {"values": [_to_otel_value(v) for v in value]}}
    if isinstance(value, dict):
        return {
            "kvlistValue": {
                "values": [{"key": str(k), "value": _to_otel_value(v)} for k, v in value.items()]
            }
        }
    return {"stringValue": str(value)}


def _to_otel_attributes(attributes: dict[str, Any]) -> list[dict[str, Any]]:
    return [{"key": str(key), "value": _to_otel_value(value)} for key, value in attributes.items()]


def _parse_status_code(raw: str | None) -> int:
    if raw is None:
        return 0
    value = str(raw).strip().upper()
    if value.isdigit():
        return int(value)
    if value in {"OK"}:
        return 1
    if value in {"ERROR"}:
        return 2
    return 0


def _parse_span_kind(raw: str | None) -> int:
    if raw is None:
        return 1
    value = str(raw).strip().upper()
    if value.isdigit():
        return int(value)
    if value == "INTERNAL":
        return 1
    if value == "SERVER":
        return 2
    if value == "CLIENT":
        return 3
    if value == "PRODUCER":
        return 4
    if value == "CONSUMER":
        return 5
    return 1


def _read_batch(
    conn: psycopg.Connection[Any],
    *,
    batch_size: int,
) -> tuple[datetime, list[_SpanRow], list[_EventRow], list[_LinkRow]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO otel.export_watermarks (destination)
            VALUES (%s)
            ON CONFLICT (destination) DO NOTHING
            """,
            (_DESTINATION,),
        )
        cur.execute(
            "SELECT watermark_time FROM otel.export_watermarks WHERE destination = %s",
            (_DESTINATION,),
        )
        watermark_time = cur.fetchone()[0]
        cur.execute(
            """
            SELECT
                s.trace_id,
                s.span_id,
                s.parent_span_id,
                s.span_name,
                s.service_name,
                t.environment,
                s.scope_name,
                s.scope_version,
                s.scope_attributes,
                s.kind,
                s.start_time,
                s.end_time,
                s.status_code,
                s.status_message,
                s.attributes,
                t.trace_name,
                t.user_id,
                t.session_id,
                t.trace_input,
                t.trace_output,
                t.resource_attributes,
                s.ingestion_time
            FROM otel.spans s
            LEFT JOIN otel.traces t
              ON t.trace_id = s.trace_id
            WHERE s.ingestion_time > %s
            ORDER BY s.ingestion_time ASC
            LIMIT %s
            """,
            (watermark_time, batch_size),
        )
        rows = cur.fetchall()

    spans = [
        _SpanRow(
            trace_id=row[0],
            span_id=row[1],
            parent_span_id=row[2],
            span_name=row[3],
            service_name=row[4],
            environment=row[5],
            scope_name=row[6],
            scope_version=row[7],
            scope_attributes=row[8] or {},
            kind=row[9],
            start_time=row[10],
            end_time=row[11],
            status_code=row[12],
            status_message=row[13],
            attributes=row[14] or {},
            trace_name=row[15],
            user_id=row[16],
            session_id=row[17],
            trace_input=row[18],
            trace_output=row[19],
            resource_attributes=row[20] or {},
            ingestion_time=row[21],
        )
        for row in rows
    ]
    if not spans:
        return watermark_time, [], [], []

    span_ids_by_trace: dict[str, set[str]] = defaultdict(set)
    for span in spans:
        span_ids_by_trace[span.trace_id].add(span.span_id)

    events: list[_EventRow] = []
    links: list[_LinkRow] = []
    with conn.cursor() as cur:
        for trace_id, span_ids in span_ids_by_trace.items():
            cur.execute(
                """
                SELECT trace_id, span_id, event_time, event_name, attributes
                FROM otel.span_events
                WHERE trace_id = %s
                  AND span_id = ANY(%s)
                ORDER BY event_time ASC
                """,
                (trace_id, list(span_ids)),
            )
            for row in cur.fetchall():
                events.append(
                    _EventRow(
                        trace_id=row[0],
                        span_id=row[1],
                        event_time=row[2],
                        event_name=row[3],
                        attributes=row[4] or {},
                    )
                )

            cur.execute(
                """
                SELECT trace_id, span_id, linked_trace_id, linked_span_id, attributes
                FROM otel.span_links
                WHERE trace_id = %s
                  AND span_id = ANY(%s)
                """,
                (trace_id, list(span_ids)),
            )
            for row in cur.fetchall():
                links.append(
                    _LinkRow(
                        trace_id=row[0],
                        span_id=row[1],
                        linked_trace_id=row[2],
                        linked_span_id=row[3],
                        attributes=row[4] or {},
                    )
                )

    return watermark_time, spans, events, links


def _build_otlp_payload(
    spans: list[_SpanRow],
    events: list[_EventRow],
    links: list[_LinkRow],
    *,
    gcp_project_id: str,
) -> dict[str, Any]:
    def _enriched_attributes(span: _SpanRow) -> dict[str, Any]:
        attrs = dict(span.attributes)
        if span.parent_span_id is None:
            if span.trace_name:
                attrs.setdefault("langfuse.trace.name", span.trace_name)
            if span.user_id:
                attrs.setdefault("user.id", span.user_id)
            if span.session_id:
                attrs.setdefault("session.id", span.session_id)
            if span.trace_input:
                attrs.setdefault("langfuse.trace.input", span.trace_input)
            if span.trace_output:
                attrs.setdefault("langfuse.trace.output", span.trace_output)
        return attrs

    events_by_span: dict[tuple[str, str], list[_EventRow]] = defaultdict(list)
    for event in events:
        events_by_span[(event.trace_id, event.span_id)].append(event)

    links_by_span: dict[tuple[str, str], list[_LinkRow]] = defaultdict(list)
    for link in links:
        links_by_span[(link.trace_id, link.span_id)].append(link)

    spans_by_resource: dict[tuple[str, str | None, str, str | None, str | None, str], list[dict[str, Any]]] = defaultdict(list)

    for span in spans:
        key = (
            span.service_name,
            span.environment,
            json.dumps(span.resource_attributes, sort_keys=True),
            span.scope_name,
            span.scope_version,
            json.dumps(span.scope_attributes, sort_keys=True),
        )
        event_payload = [
            {
                "timeUnixNano": _to_unix_nanos(event.event_time),
                "name": event.event_name,
                "attributes": _to_otel_attributes(event.attributes),
            }
            for event in events_by_span.get((span.trace_id, span.span_id), [])
        ]
        link_payload = [
            {
                "traceId": _hex_id_to_b64(link.linked_trace_id, 16),
                "spanId": _hex_id_to_b64(link.linked_span_id, 8),
                "attributes": _to_otel_attributes(link.attributes),
            }
            for link in links_by_span.get((span.trace_id, span.span_id), [])
        ]
        end_time = span.end_time or span.start_time
        span_attributes = _enriched_attributes(span)

        spans_by_resource[key].append(
            {
                "traceId": _hex_id_to_b64(span.trace_id, 16),
                "spanId": _hex_id_to_b64(span.span_id, 8),
                "parentSpanId": _hex_id_to_b64(span.parent_span_id or "", 8),
                "name": span.span_name,
                "kind": _parse_span_kind(span.kind),
                "startTimeUnixNano": _to_unix_nanos(span.start_time),
                "endTimeUnixNano": _to_unix_nanos(end_time),
                "attributes": _to_otel_attributes(span_attributes),
                "events": event_payload,
                "links": link_payload,
                "status": {
                    "code": _parse_status_code(span.status_code),
                    "message": span.status_message or "",
                },
            }
        )

    resource_spans: list[dict[str, Any]] = []
    for (service_name, environment, resource_json, scope_name, scope_version, scope_attr_json), grouped_spans in spans_by_resource.items():
        resource_attrs = json.loads(resource_json)
        scope_attrs = json.loads(scope_attr_json)
        resource_attrs.setdefault("service.name", service_name)
        if environment:
            resource_attrs.setdefault("deployment.environment", environment)
        if gcp_project_id:
            resource_attrs.setdefault("gcp.project_id", gcp_project_id)
        resource_spans.append(
            {
                "resource": {"attributes": _to_otel_attributes(resource_attrs)},
                "scopeSpans": [
                    {
                        "scope": {
                            "name": scope_name or "unknown-scope",
                            "version": scope_version or "",
                            "attributes": _to_otel_attributes(scope_attrs),
                        },
                        "spans": grouped_spans,
                    }
                ],
            }
        )
    return {"resourceSpans": resource_spans}


def _resolve_access_token(explicit_access_token: str) -> str:
    if explicit_access_token:
        return explicit_access_token

    if google is None:
        raise RuntimeError(
            "No Cloud Trace access token provided and google-auth is unavailable. "
            "Set CLOUD_TRACE_ACCESS_TOKEN or install google-auth."
        )

    scopes = ["https://www.googleapis.com/auth/trace.append"]
    credentials, _ = google.auth.default(scopes=scopes)
    if not credentials.valid or credentials.expired:
        credentials.refresh(Request())
    token = getattr(credentials, "token", None)
    if not token:
        raise RuntimeError(
            "Failed to acquire Google access token. "
            "Set CLOUD_TRACE_ACCESS_TOKEN or configure ADC."
        )
    return str(token)


def _cloud_trace_headers(access_token: str, quota_project_id: str) -> dict[str, str]:
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/x-protobuf",
    }
    if quota_project_id:
        headers["x-goog-user-project"] = quota_project_id
    return headers


def _record_attempts(
    conn: psycopg.Connection[Any],
    *,
    spans: list[_SpanRow],
    success: bool,
    status_code: str | None,
    error_message: str | None,
) -> None:
    if not spans:
        return
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO otel.export_attempts (
                destination,
                trace_id,
                span_id,
                success,
                status_code,
                error_message
            ) VALUES (%s, %s, %s, %s, %s, %s)
            """,
            [
                (_DESTINATION, span.trace_id, span.span_id, success, status_code, error_message)
                for span in spans
            ],
        )


def _update_watermark(conn: psycopg.Connection[Any], watermark_time: datetime) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE otel.export_watermarks
            SET watermark_time = %s, updated_at = NOW()
            WHERE destination = %s
            """,
            (watermark_time, _DESTINATION),
        )


def _export_once(
    *,
    db_dsn: str,
    cloud_trace_otlp_endpoint: str,
    access_token: str,
    quota_project_id: str,
    gcp_project_id: str,
    batch_size: int,
    timeout_seconds: float,
    dry_run: bool,
) -> int:
    endpoint = cloud_trace_otlp_endpoint
    with psycopg.connect(db_dsn, autocommit=False) as conn:
        prior_watermark, spans, events, links = _read_batch(conn, batch_size=batch_size)
        if not spans:
            conn.rollback()
            LOGGER.info("No new spans to export.")
            return 0

        payload = _build_otlp_payload(
            spans=spans,
            events=events,
            links=links,
            gcp_project_id=gcp_project_id,
        )
        if dry_run:
            LOGGER.info(
                "Dry run: prepared %s spans in %s resourceSpans (watermark=%s).",
                len(spans),
                len(payload["resourceSpans"]),
                prior_watermark.isoformat(),
            )
            conn.rollback()
            return len(spans)

        try:
            if ExportTraceServiceRequest is None:
                raise RuntimeError(
                    "Cloud Trace export requires OTLP protobuf definitions; "
                    "install opentelemetry-proto dependencies."
                )
            request_proto = ExportTraceServiceRequest()
            ParseDict(payload, request_proto)
            body = request_proto.SerializeToString()
            response = httpx.post(
                endpoint,
                headers=_cloud_trace_headers(access_token=access_token, quota_project_id=quota_project_id),
                content=body,
                timeout=timeout_seconds,
            )
            response.raise_for_status()
        except Exception as exc:
            response_body = ""
            response_obj = getattr(exc, "response", None)
            if response_obj is not None:
                try:
                    response_body = response_obj.text[:2000]
                except Exception:
                    response_body = ""
            _record_attempts(
                conn,
                spans=spans,
                success=False,
                status_code=getattr(getattr(exc, "response", None), "status_code", None),
                error_message=f"{exc} | response_body={response_body}" if response_body else str(exc),
            )
            conn.commit()
            raise

        max_ingestion_time = max(span.ingestion_time for span in spans)
        _record_attempts(
            conn,
            spans=spans,
            success=True,
            status_code=str(response.status_code),
            error_message=None,
        )
        _update_watermark(conn, max_ingestion_time)
        conn.commit()
        LOGGER.info(
            "Exported %s spans to Cloud Trace (%s). Watermark advanced to %s.",
            len(spans),
            response.status_code,
            max_ingestion_time.isoformat(),
        )
        return len(spans)


def _arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Export spans from Postgres OTEL schema to Cloud Trace OTLP endpoint.")
    parser.add_argument(
        "--db-dsn",
        default=os.getenv(
            "OTEL_POSTGRES_DSN",
            "postgresql://otel_user:otel_password@localhost:5432/otel_observability",
        ),
    )
    parser.add_argument(
        "--cloud-trace-otlp-endpoint",
        default=os.getenv("CLOUD_TRACE_OTLP_ENDPOINT", "https://telemetry.googleapis.com/v1/traces"),
    )
    parser.add_argument("--cloud-trace-access-token", default=os.getenv("CLOUD_TRACE_ACCESS_TOKEN", ""))
    parser.add_argument("--cloud-trace-quota-project", default=os.getenv("CLOUD_TRACE_QUOTA_PROJECT", ""))
    parser.add_argument(
        "--cloud-trace-project-id",
        default=os.getenv("CLOUD_TRACE_PROJECT_ID", os.getenv("GOOGLE_CLOUD_PROJECT", "")),
    )
    parser.add_argument("--batch-size", type=int, default=int(os.getenv("CLOUD_TRACE_EXPORT_BATCH_SIZE", "200")))
    parser.add_argument("--timeout-seconds", type=float, default=float(os.getenv("CLOUD_TRACE_EXPORT_TIMEOUT_SECONDS", "20")))
    parser.add_argument("--poll-interval-seconds", type=float, default=float(os.getenv("CLOUD_TRACE_EXPORT_POLL_SECONDS", "5")))
    parser.add_argument("--continuous", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--log-level", default=os.getenv("CLOUD_TRACE_EXPORT_LOG_LEVEL", "INFO"))
    return parser


def main() -> None:
    args = _arg_parser().parse_args()
    logging.basicConfig(level=getattr(logging, str(args.log_level).upper(), logging.INFO))

    access_token = ""
    if not args.dry_run:
        try:
            access_token = _resolve_access_token(args.cloud_trace_access_token)
        except Exception as exc:
            raise SystemExit(
                "Cloud Trace export requires authentication via CLOUD_TRACE_ACCESS_TOKEN "
                f"or Application Default Credentials. Details: {exc}"
            ) from exc

    while True:
        exported = _export_once(
            db_dsn=args.db_dsn,
            cloud_trace_otlp_endpoint=args.cloud_trace_otlp_endpoint,
            access_token=access_token,
            quota_project_id=args.cloud_trace_quota_project,
            gcp_project_id=(args.cloud_trace_project_id or args.cloud_trace_quota_project),
            batch_size=args.batch_size,
            timeout_seconds=args.timeout_seconds,
            dry_run=args.dry_run,
        )
        if exported == 0:
            if args.continuous:
                time.sleep(args.poll_interval_seconds)
                continue
            break


if __name__ == "__main__":
    main()
