# Google ADK Multi-Agent UK Resell Lead System

This project runs a Google ADK-oriented sourcing pipeline to identify UK resale opportunities from Japanese trading-card retailers.

## Current Scope

- Category focus: trading cards (Pokemon, One Piece, Yu-Gi-Oh, Digimon, related TCG products).
- Active sources:
  - HobbyLink Japan (`https://www.hlj.com/`)
  - Nin-Nin-Game (`https://www.nin-nin-game.com/en/`)
- Output: JSON (optional) plus a formatted timestamped HTML report.

## Agent Design

The ADK graph is a simple orchestrator + specialists sequence:

1. `item_sourcing_agent`
   - Calls `find_candidate_items` for configured sources.
2. `profitability_agent`
   - Calls `assess_profitability_against_ebay` for each candidate.
3. `report_writer_agent`
   - Produces a structured lead report.
4. `uk_resell_orchestrator`
   - Parent `SequentialAgent` combining all stages.

## Project Layout

- `src/uk_resell_adk/agents.py` – ADK multi-agent construction
- `src/uk_resell_adk/tools.py` – tool functions and source diagnostics
- `src/uk_resell_adk/sources/` – source adapters and shared parsing helpers
- `src/uk_resell_adk/html_renderer.py` – HTML report generation
- `src/uk_resell_adk/main.py` – local dry-run CLI entrypoint
- `src/uk_resell_adk/app.py` – exposes `root_agent` for ADK runtime

## Quick Start

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
python -m uk_resell_adk.main --json
```

## CLI Flags

- `--json` print workflow payload as JSON
- `--html-out <path>` write report to a fixed path
- `--allow-fallback` allow static fallback catalog items when live scrape fails
- `--strict-live` fail run when a required source has zero live candidates
- `--debug-sources` write raw source snapshots to debug folder
- `--debug-dir <path>` set debug snapshot directory (default `debug/sources`)

By default, reports are written to unique files like `reports/uk_resell_report_20260217_204113.html`.

## Tracing (LangSmith + Langfuse)

Tracing is optional and can run to both providers concurrently.

```bash
export ENABLE_OTEL_TRACING="true"        # defaults true
export ENABLE_LANGSMITH_TRACING="true"   # optional, defaults true
export ENABLE_LANGFUSE_TRACING="true"    # optional, defaults true

export OTEL_EXPORTER_OTLP_PROTOCOL="http/protobuf"
export OTEL_EXPORTER_OTLP_TRACES_ENDPOINT="http://127.0.0.1:4318/v1/traces" # defaults to local ingest
export OTEL_SERVICE_NAME="uk-resell-adk" # optional
export OTEL_CAPTURE_FUNCTION_ARGS="true"   # optional, defaults true
export OTEL_CAPTURE_FUNCTION_RESULT="true" # optional, defaults true
export OTEL_MAX_ATTR_LEN="4000"            # optional max JSON attr length

export LANGSMITH_API_KEY="your-langsmith-api-key"
export LANGSMITH_PROJECT="uk-resell-adk" # optional

export LANGFUSE_PUBLIC_KEY="your-langfuse-public-key"
export LANGFUSE_SECRET_KEY="your-langfuse-secret-key"
export LANGFUSE_BASE_URL="https://cloud.langfuse.com" # optional
```

Traced spans include:
- `run_local_dry_run`
- `build_multi_agent_system`
- sourcing/profitability tool calls

## ADK Runtime

Use `uk_resell_adk.app:root_agent` as the ADK entrypoint.

## Local Postgres Trace Warehouse

This repo includes a local Postgres setup for storing OpenTelemetry traces before forwarding to external tools.

Initialize and start:

```bash
./scripts/postgres/start_postgres.sh
```

Open a SQL shell:

```bash
./scripts/postgres/psql_postgres.sh
```

Stop container:

```bash
./scripts/postgres/stop_postgres.sh
```

Stop and delete all data:

```bash
./scripts/postgres/stop_postgres.sh --wipe-data
```

Schema is auto-loaded on first start from:

- `infra/postgres/init/001_otel_schema.sql`

Tables are created under schema `otel`:

- `otel.traces`
- `otel.spans` (partitioned by month)
- `otel.span_events` (partitioned by month)
- `otel.span_links`
- `otel.export_watermarks`
- `otel.export_attempts`

Schema visualization:

- `docs/otel_schema_visualization.md`

### Start OTLP Ingest (Postgres)

Run the ingest service that accepts OTLP JSON and OTLP protobuf traces and writes to Postgres:

```bash
./scripts/postgres/start_otel_ingest.sh
```

Default endpoint:

- `http://127.0.0.1:4318/v1/traces`

Example exporter settings for applications sending OTLP/HTTP protobuf:

```bash
export OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf
export OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://127.0.0.1:4318/v1/traces
```

### Export Postgres Traces To Langfuse

After traces are stored in Postgres, export them to Langfuse for visualization:

```bash
export LANGFUSE_PUBLIC_KEY="pk-lf-..."
export LANGFUSE_SECRET_KEY="sk-lf-..."
export LANGFUSE_BASE_URL="https://cloud.langfuse.com" # or your self-hosted URL

./scripts/postgres/export_to_langfuse.sh
```

By default, one run drains all currently pending spans in batches.

Run continuously:

```bash
./scripts/postgres/export_to_langfuse.sh --continuous
```

Dry-run payload build (no network write):

```bash
./scripts/postgres/export_to_langfuse.sh --dry-run
```

### Export Postgres Traces To Google Cloud Trace

After traces are stored in Postgres, export the same OTLP spans to Cloud Trace:

```bash
# Option A (recommended): use Application Default Credentials (ADC)
gcloud auth application-default login

# Optional: if using user credentials, set quota project for Cloud APIs
export CLOUD_TRACE_QUOTA_PROJECT="your-gcp-project-id"
export CLOUD_TRACE_OTLP_ENDPOINT="https://telemetry.googleapis.com/v1/traces"

./scripts/postgres/export_to_cloud_trace.sh
```

By default, one run drains all currently pending spans in batches.

Option B, pass an explicit bearer token:

```bash
export CLOUD_TRACE_ACCESS_TOKEN="$(gcloud auth application-default print-access-token)"
./scripts/postgres/export_to_cloud_trace.sh
```

Run continuously:

```bash
./scripts/postgres/export_to_cloud_trace.sh --continuous
```

Dry-run payload build (no network write):

```bash
./scripts/postgres/export_to_cloud_trace.sh --dry-run
```

### Export Postgres Traces To Dynatrace

After traces are stored in Postgres, export OTLP traces to Dynatrace:

```bash
export DYNATRACE_OTLP_ENDPOINT="https://<environment-id>.live.dynatrace.com/api/v2/otlp/v1/traces"
export DYNATRACE_API_TOKEN="dt0c01...."

./scripts/postgres/export_to_dynatrace.sh
```

By default, one run drains all currently pending spans in batches.

Run continuously:

```bash
./scripts/postgres/export_to_dynatrace.sh --continuous
```

Dry-run payload build (no network write):

```bash
./scripts/postgres/export_to_dynatrace.sh --dry-run
```
