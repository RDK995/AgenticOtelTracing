# Architecture Diagram

```mermaid
flowchart LR
    U[User / CLI] --> R["./run.sh local --json"]
    R --> M["uk_resell_adk.main.run_local_dry_run()"]

    subgraph AG["Agentic Workflow"]
      A1[item_sourcing_agent]
      A2[profitability_agent]
      A3[report_writer_agent]
      O[uk_resell_orchestrator]
      O --> A1 --> A2 --> A3
    end

    M --> AG

    subgraph TR["Tracing Instrumentation"]
      T1["@traceable"]
      T2["start_trace_span() / add_trace_event() / add_trace_attributes()"]
    end

    AG --> T1
    AG --> T2

    T1 --> OTLP["OTLP HTTP Export<br/>http://127.0.0.1:4318/v1/traces"]
    T2 --> OTLP

    subgraph WH["Local Trace Warehouse"]
      ING["otel_ingest.py"]
      DB[(Postgres schema: otel)]
      TB1["otel.traces"]
      TB2["otel.spans / span_events / span_links"]
      TB3["otel.export_watermarks / export_attempts"]
      ING --> DB
      DB --> TB1
      DB --> TB2
      DB --> TB3
    end

    OTLP --> ING

    subgraph EX["Exporters (Incremental, watermark-based)"]
      E1["export_to_langfuse.sh<br/>langfuse_exporter.py"]
      E2["export_to_cloud_trace.sh<br/>cloud_trace_exporter.py"]
      E3["export_to_dynatrace.sh<br/>dynatrace_exporter.py"]
    end

    DB --> E1 --> LF[Langfuse]
    DB --> E2 --> GCT[Google Cloud Trace]
    DB --> E3 --> DT[Dynatrace]
```

## Notes

- Exporters read new rows since each destination watermark and advance on success.
- One workflow run can be exported to multiple destinations from the same Postgres source data.
- `otel.export_attempts` provides per-span delivery audit/error logs.
