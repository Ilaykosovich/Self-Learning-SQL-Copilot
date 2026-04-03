from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST, CollectorRegistry
from fastapi import APIRouter, HTTPException
from fastapi.responses import Response

REGISTRY = CollectorRegistry(auto_describe=True)

REQUESTS_TOTAL = Counter(
    "orchestrator_requests_total",
    "Total number of requests",
    ["endpoint", "method", "status"],
    registry=REGISTRY,
)

REQUEST_LATENCY = Histogram(
    "orchestrator_request_latency_seconds",
    "Request latency in seconds",
    ["endpoint", "method"],
    registry=REGISTRY,
)

LLM_LATENCY = Histogram(
    "orchestrator_llm_latency_seconds",
    "LLM invocation latency in seconds",
    ["model", "mode"],
    registry=REGISTRY,
)

LLM_ERRORS_TOTAL = Counter(
    "orchestrator_llm_errors_total",
    "Total LLM errors",
    ["model", "mode", "error_type"],
    registry=REGISTRY,
)

LLM_TTFT = Histogram(
    "orchestrator_llm_ttft_seconds",
    "Time to first token from LLM (seconds)",
    ["model"],
    registry=REGISTRY,
)

INFLIGHT = Gauge(
    "orchestrator_inflight_requests",
    "Number of in-flight requests",
    registry=REGISTRY,
)

metrics_router = APIRouter()


@metrics_router.get("/metrics")
def metrics():
    return Response(generate_latest(REGISTRY), media_type=CONTENT_TYPE_LATEST)