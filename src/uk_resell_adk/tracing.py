from __future__ import annotations

"""Optional tracing integration helpers.

This module supports concurrent tracing to LangSmith and Langfuse.
All call sites use one decorator API (`traceable`) so provider wiring remains
centralized and easy to evolve.
"""

import atexit
import contextlib
import functools
import json
import os
import sys
from collections.abc import Callable
from typing import Any, TypeVar, cast


F = TypeVar("F", bound=Callable[..., Any])


try:
    from langsmith import traceable as _langsmith_traceable
except Exception:
    _langsmith_traceable = None

try:
    # Langfuse exposes a decorator API for observation spans.
    from langfuse import observe as _langfuse_observe
except Exception:
    _langfuse_observe = None

try:
    # Optional client access for flush() on process exit.
    from langfuse import get_client as _langfuse_get_client
except Exception:
    _langfuse_get_client = None

try:
    # Preferred way to assign user/session for nested observations.
    from langfuse import propagate_attributes as _langfuse_propagate_attributes
except Exception:
    _langfuse_propagate_attributes = None

try:
    # Context API supports setting session/user attributes on the current trace.
    from langfuse.decorators import langfuse_context as _langfuse_context
except Exception:
    _langfuse_context = None

try:
    from opentelemetry import trace as _otel_trace
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter as _otel_otlp_span_exporter
    from opentelemetry.sdk.resources import Resource as _otel_resource
    from opentelemetry.sdk.trace import TracerProvider as _otel_tracer_provider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor as _otel_batch_span_processor
    from opentelemetry.trace import Status as _otel_status
    from opentelemetry.trace import StatusCode as _otel_status_code
except Exception:
    _otel_trace = None
    _otel_otlp_span_exporter = None
    _otel_resource = None
    _otel_tracer_provider = None
    _otel_batch_span_processor = None
    _otel_status = None
    _otel_status_code = None


_AEXIT_REGISTERED = False
_OTEL_CONFIGURED = False


def _env_truthy(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _compose_decorators(decorators: list[Callable[[F], F]]) -> Callable[[F], F]:
    if not decorators:

        def _identity(func: F) -> F:
            return func

        return _identity

    def _decorator(func: F) -> F:
        wrapped = func
        # Apply in reverse so the first provider in list is outermost.
        for dec in reversed(decorators):
            wrapped = dec(wrapped)
        return wrapped

    return _decorator


def _safe_json(value: Any, *, max_len: int) -> str:
    try:
        encoded = json.dumps(value, default=str, ensure_ascii=True, separators=(",", ":"))
    except Exception:
        encoded = repr(value)
    if len(encoded) > max_len:
        return encoded[: max_len - 3] + "..."
    return encoded


def _langfuse_as_type(run_type: Any) -> str | None:
    value = str(run_type).strip().lower() if run_type is not None else ""
    if value in {"tool", "chain", "agent"}:
        return value
    return None


def _span_is_recording(span: Any) -> bool:
    if span is None:
        return False
    is_recording = getattr(span, "is_recording", None)
    if callable(is_recording):
        try:
            return bool(is_recording())
        except Exception:
            return False
    return False


def _langfuse_trace_identity_decorator() -> Callable[[F], F]:
    def _decorator(func: F) -> F:
        if _langfuse_context is None and _langfuse_propagate_attributes is None:
            return func

        @functools.wraps(func)
        def _wrapped(*f_args: Any, **f_kwargs: Any) -> Any:
            user_id = os.getenv("LANGFUSE_USER_ID")
            session_id = os.getenv("LANGFUSE_SESSION_ID")
            if not (user_id or session_id):
                return func(*f_args, **f_kwargs)

            if _langfuse_propagate_attributes is not None:
                try:
                    propagated_attributes = _langfuse_propagate_attributes(user_id=user_id, session_id=session_id)
                except Exception:
                    propagated_attributes = None
                if propagated_attributes is not None:
                    with propagated_attributes:
                        return func(*f_args, **f_kwargs)

            if _langfuse_context is not None:
                try:
                    _langfuse_context.update_current_trace(user_id=user_id, session_id=session_id)
                except Exception:
                    pass
            return func(*f_args, **f_kwargs)

        return cast(F, _wrapped)

    return _decorator


def _otel_traceable_decorator(*args: Any, **kwargs: Any) -> Callable[[F], F]:
    span_name = kwargs.get("name")
    if not span_name and args:
        span_name = args[0]
    if not span_name:
        span_name = "unnamed-span"
    span_name = str(span_name)

    run_type = kwargs.get("run_type")
    capture_args = _env_truthy("OTEL_CAPTURE_FUNCTION_ARGS", True)
    capture_result = _env_truthy("OTEL_CAPTURE_FUNCTION_RESULT", True)
    max_attr_len = int(os.getenv("OTEL_MAX_ATTR_LEN", "4000"))

    def _decorator(func: F) -> F:
        @functools.wraps(func)
        def _wrapped(*f_args: Any, **f_kwargs: Any) -> Any:
            configured = (
                _OTEL_CONFIGURED
                and _otel_trace is not None
                and _otel_status is not None
                and _otel_status_code is not None
            )
            if not configured:
                return func(*f_args, **f_kwargs)

            tracer = _otel_trace.get_tracer("uk_resell_adk")
            parent_span = _otel_trace.get_current_span()
            is_root_span = not _span_is_recording(parent_span)
            langfuse_type = _langfuse_as_type(run_type) or "span"
            with tracer.start_as_current_span(span_name) as span:
                span.set_attribute("code.function", func.__name__)
                span.set_attribute("code.module", func.__module__)
                if run_type is not None:
                    span.set_attribute("adk.run_type", str(run_type))
                span.set_attribute("langfuse.observation.type", langfuse_type)
                if is_root_span:
                    span.set_attribute("langfuse.trace.name", span_name)
                user_id = os.getenv("LANGFUSE_USER_ID")
                session_id = os.getenv("LANGFUSE_SESSION_ID")
                if user_id:
                    span.set_attribute("enduser.id", user_id)
                    span.set_attribute("user.id", user_id)
                    span.set_attribute("langfuse.user.id", user_id)
                if session_id:
                    span.set_attribute("session.id", session_id)
                    span.set_attribute("langfuse.session.id", session_id)
                if capture_args:
                    input_json = _safe_json({"args": f_args, "kwargs": f_kwargs}, max_len=max_attr_len)
                    span.set_attribute(
                        "adk.args_json",
                        input_json,
                    )
                    span.set_attribute("langfuse.observation.input", input_json)
                    if is_root_span:
                        span.set_attribute("langfuse.trace.input", input_json)
                try:
                    result = func(*f_args, **f_kwargs)
                    if capture_result:
                        output_json = _safe_json(result, max_len=max_attr_len)
                        span.set_attribute("adk.result_json", output_json)
                        span.set_attribute("langfuse.observation.output", output_json)
                        if is_root_span:
                            span.set_attribute("langfuse.trace.output", output_json)
                    span.set_status(_otel_status(_otel_status_code.OK))
                    return result
                except Exception as exc:
                    span.record_exception(exc)
                    span.set_attribute("error.type", exc.__class__.__name__)
                    span.set_attribute("error.message", str(exc)[:max_attr_len])
                    span.set_attribute("langfuse.observation.level", "ERROR")
                    span.set_attribute("langfuse.observation.status_message", str(exc)[:max_attr_len])
                    span.set_status(_otel_status(_otel_status_code.ERROR))
                    raise

        return cast(F, _wrapped)

    return _decorator


def traceable(*args: Any, **kwargs: Any) -> Callable[[F], F]:
    """Return a decorator that fans out traces to enabled providers."""
    decorators: list[Callable[[F], F]] = []

    otel_enabled = _env_truthy("ENABLE_OTEL_TRACING", True)
    if otel_enabled and _otel_trace is not None:
        decorators.append(_otel_traceable_decorator(*args, **kwargs))

    langfuse_enabled = _env_truthy("ENABLE_LANGFUSE_TRACING", True) and bool(os.getenv("LANGFUSE_PUBLIC_KEY")) and bool(
        os.getenv("LANGFUSE_SECRET_KEY")
    )
    if langfuse_enabled and _langfuse_observe is not None:
        lf_kwargs = dict(kwargs)
        as_type = _langfuse_as_type(kwargs.get("run_type"))
        lf_kwargs.pop("run_type", None)
        if as_type is not None:
            lf_kwargs["as_type"] = as_type
        decorators.append(cast(Callable[[F], F], _langfuse_observe(*args, **lf_kwargs)))
        decorators.append(_langfuse_trace_identity_decorator())

    langsmith_enabled = _env_truthy("ENABLE_LANGSMITH_TRACING", True) and bool(os.getenv("LANGSMITH_API_KEY"))
    if langsmith_enabled and _langsmith_traceable is not None:
        decorators.append(cast(Callable[[F], F], _langsmith_traceable(*args, **kwargs)))

    return _compose_decorators(decorators)


def add_trace_attributes(attributes: dict[str, Any]) -> None:
    """Attach attributes to the current active OTel span if available."""
    if _otel_trace is None:
        return
    try:
        span = _otel_trace.get_current_span()
    except Exception:
        return
    if span is None:
        return
    is_recording = getattr(span, "is_recording", None)
    if callable(is_recording) and not is_recording():
        return
    max_attr_len = int(os.getenv("OTEL_MAX_ATTR_LEN", "4000"))
    for key, value in attributes.items():
        if value is None:
            continue
        if isinstance(value, (str, bool, int, float)):
            serialized = value
        else:
            serialized = _safe_json(value, max_len=max_attr_len)
        try:
            span.set_attribute(str(key), serialized)
            key_str = str(key)
            if not key_str.startswith("langfuse."):
                span.set_attribute(f"langfuse.observation.metadata.{key_str}", serialized)
        except Exception:
            continue


def add_trace_event(name: str, attributes: dict[str, Any] | None = None) -> None:
    """Add an event to the current active OTel span if available."""
    if _otel_trace is None:
        return
    try:
        span = _otel_trace.get_current_span()
    except Exception:
        return
    if span is None:
        return
    is_recording = getattr(span, "is_recording", None)
    if callable(is_recording) and not is_recording():
        return
    max_attr_len = int(os.getenv("OTEL_MAX_ATTR_LEN", "4000"))
    event_attributes: dict[str, Any] = {}
    for key, value in (attributes or {}).items():
        if value is None:
            continue
        if isinstance(value, (str, bool, int, float)):
            event_attributes[str(key)] = value
        else:
            event_attributes[str(key)] = _safe_json(value, max_len=max_attr_len)
    try:
        span.add_event(name=name, attributes=event_attributes)
    except Exception:
        return


def start_trace_span(name: str, attributes: dict[str, Any] | None = None) -> Any:
    """Start a nested span when OTel is configured, else return a no-op context manager."""
    if _otel_trace is None:
        return contextlib.nullcontext()
    try:
        tracer = _otel_trace.get_tracer("uk_resell_adk")
        ctx = tracer.start_as_current_span(name)
    except Exception:
        return contextlib.nullcontext()

    class _SpanCtx:
        def __enter__(self) -> Any:
            span = ctx.__enter__()
            if attributes:
                add_trace_attributes(attributes)
            return span

        def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> Any:
            return ctx.__exit__(exc_type, exc, tb)

    return _SpanCtx()


def _flush_tracing_clients() -> None:
    # Best-effort flush to reduce dropped spans on short-lived CLI processes.
    if _langfuse_get_client is not None:
        try:
            _langfuse_get_client().flush()
        except Exception:
            pass
    if _OTEL_CONFIGURED and _otel_trace is not None:
        try:
            provider = _otel_trace.get_tracer_provider()
            force_flush = getattr(provider, "force_flush", None)
            if callable(force_flush):
                force_flush()
            shutdown = getattr(provider, "shutdown", None)
            if callable(shutdown):
                shutdown()
        except Exception:
            pass


def configure_langsmith(project_name: str = "uk-resell-adk") -> None:
    """Backward-compatible alias for legacy call sites.

    This now configures both LangSmith and Langfuse tracing defaults.
    """
    configure_tracing(project_name=project_name)


def configure_tracing(project_name: str = "uk-resell-adk") -> None:
    """Enable tracing defaults for LangSmith and Langfuse concurrently."""
    global _AEXIT_REGISTERED, _OTEL_CONFIGURED

    otel_enabled = _env_truthy("ENABLE_OTEL_TRACING", True)
    if otel_enabled:
        os.environ.setdefault("OTEL_SERVICE_NAME", project_name)
        os.environ.setdefault("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "http://127.0.0.1:4318/v1/traces")
        os.environ.setdefault("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf")
        if (
            _otel_trace is None
            or _otel_otlp_span_exporter is None
            or _otel_resource is None
            or _otel_tracer_provider is None
            or _otel_batch_span_processor is None
        ):
            print(
                "Warning: OpenTelemetry tracing enabled but opentelemetry SDK/exporter packages are not installed; OTel tracing is disabled.",
                file=sys.stderr,
            )
        elif not _OTEL_CONFIGURED:
            endpoint = os.environ["OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"]
            service_name = os.environ["OTEL_SERVICE_NAME"]
            resource = _otel_resource.create({"service.name": service_name})
            provider = _otel_tracer_provider(resource=resource)
            exporter = _otel_otlp_span_exporter(endpoint=endpoint)
            provider.add_span_processor(_otel_batch_span_processor(exporter))
            _otel_trace.set_tracer_provider(provider)
            _OTEL_CONFIGURED = True

    langsmith_enabled = _env_truthy("ENABLE_LANGSMITH_TRACING", True) and bool(os.getenv("LANGSMITH_API_KEY"))
    if langsmith_enabled:
        os.environ.setdefault("LANGSMITH_TRACING", "true")
        os.environ.setdefault("LANGSMITH_PROJECT", project_name)
        if _langsmith_traceable is None:
            print(
                "Warning: LangSmith tracing enabled but langsmith SDK is not installed; LangSmith tracing is disabled.",
                file=sys.stderr,
            )

    langfuse_enabled = _env_truthy("ENABLE_LANGFUSE_TRACING", True) and bool(os.getenv("LANGFUSE_PUBLIC_KEY")) and bool(
        os.getenv("LANGFUSE_SECRET_KEY")
    )
    if langfuse_enabled:
        os.environ.setdefault("LANGFUSE_TRACING_ENABLED", "true")
        os.environ.setdefault("LANGFUSE_HOST", os.getenv("LANGFUSE_BASE_URL", "https://cloud.langfuse.com"))
        if _langfuse_observe is None:
            print(
                "Warning: Langfuse tracing enabled but langfuse SDK is not installed; Langfuse tracing is disabled.",
                file=sys.stderr,
            )

    if not _AEXIT_REGISTERED:
        atexit.register(_flush_tracing_clients)
        _AEXIT_REGISTERED = True
