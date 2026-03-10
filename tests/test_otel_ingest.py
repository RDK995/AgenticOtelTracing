from __future__ import annotations

from google.protobuf.json_format import MessageToDict
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest
from uk_resell_adk.otel_ingest import _extract_rows, _parse_otlp_payload


def test_extract_rows_parses_otlp_json_payload() -> None:
    payload = {
        "resourceSpans": [
            {
                "resource": {
                    "attributes": [
                        {"key": "service.name", "value": {"stringValue": "uk-resell-adk"}},
                        {"key": "deployment.environment", "value": {"stringValue": "test"}},
                    ]
                },
                "scopeSpans": [
                    {
                        "spans": [
                            {
                                "traceId": "00112233445566778899aabbccddeeff",
                                "spanId": "1111222233334444",
                                "name": "root-operation",
                                "kind": 2,
                                "startTimeUnixNano": "1741468800000000000",
                                "endTimeUnixNano": "1741468800500000000",
                                "attributes": [{"key": "http.status_code", "value": {"intValue": "200"}}],
                                "status": {"code": 1, "message": "OK"},
                                "events": [
                                    {
                                        "timeUnixNano": "1741468800100000000",
                                        "name": "db.query",
                                        "attributes": [{"key": "db.system", "value": {"stringValue": "postgresql"}}],
                                    }
                                ],
                                "links": [
                                    {
                                        "traceId": "ffeeddccbbaa99887766554433221100",
                                        "spanId": "9999888877776666",
                                        "attributes": [{"key": "link.type", "value": {"stringValue": "follows_from"}}],
                                    }
                                ],
                            }
                        ]
                    }
                ],
            }
        ]
    }

    traces, spans, events, links = _extract_rows(payload)

    assert len(traces) == 1
    assert len(spans) == 1
    assert len(events) == 1
    assert len(links) == 1

    assert traces[0].trace_id == "00112233445566778899aabbccddeeff"
    assert traces[0].service_name == "uk-resell-adk"
    assert traces[0].environment == "test"

    assert spans[0].span_id == "1111222233334444"
    assert spans[0].span_name == "root-operation"
    assert spans[0].attributes["http.status_code"] == 200

    assert events[0].event_name == "db.query"
    assert links[0].linked_span_id == "9999888877776666"


def test_parse_otlp_payload_accepts_protobuf() -> None:
    request = ExportTraceServiceRequest()
    resource_span = request.resource_spans.add()
    attr = resource_span.resource.attributes.add()
    attr.key = "service.name"
    attr.value.string_value = "test-svc"

    scope_span = resource_span.scope_spans.add()
    span = scope_span.spans.add()
    span.trace_id = bytes.fromhex("00112233445566778899aabbccddeeff")
    span.span_id = bytes.fromhex("1111222233334444")
    span.name = "root-op"
    span.kind = 2
    span.start_time_unix_nano = 1741468800000000000
    span.end_time_unix_nano = 1741468800500000000

    body = request.SerializeToString()
    payload = _parse_otlp_payload(body, "application/x-protobuf")
    assert payload == MessageToDict(request, preserving_proto_field_name=False, use_integers_for_enums=True)

    traces, spans, events, links = _extract_rows(payload)
    assert len(traces) == 1
    assert len(spans) == 1
    assert len(events) == 0
    assert len(links) == 0
    assert traces[0].service_name == "test-svc"
