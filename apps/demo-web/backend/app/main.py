from __future__ import annotations

import asyncio
import logging
import os
import time
import uuid
from contextlib import asynccontextmanager
from typing import Any

import httpx
from fastapi import FastAPI, Header, HTTPException, Query, Response, status
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest

from app.database import (
    DemoRepository,
    IdempotencyConflictError,
    RecordNotFoundError,
    canonical_payload_hash,
    event_id_for,
    event_type_id,
)
from app.models import (
    EventAccepted,
    EventRequest,
    EventStatus,
    ProductPage,
    RecommendationItem,
    RecommendationRequest,
    RecommendationResponse,
    UserPage,
)
from app.telemetry import configure_telemetry

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
LOGGER = logging.getLogger("recsys.demo_api")
REQUESTS = Counter("recsys_demo_api_requests_total", "Demo API requests", ["method", "path", "status"])
LATENCY = Histogram("recsys_demo_api_request_duration_seconds", "Demo API latency", ["method", "path"])

repository = DemoRepository()
downstream_client: httpx.AsyncClient | None = None


def inference_url() -> str:
    return os.getenv("INFERENCE_API_URL", "http://recsys-inference-api.api-serving.svc.cluster.local")


def feature_url() -> str:
    return os.getenv(
        "FEATURE_API_URL",
        "http://recsys-online-feature-api.api-serving.svc.cluster.local",
    )


@asynccontextmanager
async def lifespan(_: FastAPI):
    global downstream_client
    await repository.open()
    try:
        downstream_client = httpx.AsyncClient(
            timeout=httpx.Timeout(
                connect=float(os.getenv("DOWNSTREAM_CONNECT_TIMEOUT_SECONDS", "2")),
                read=float(os.getenv("DOWNSTREAM_READ_TIMEOUT_SECONDS", "15")),
                write=5,
                pool=5,
            ),
            # Bound the shared pool so concurrent browser requests cannot create
            # an unbounded number of downstream sockets.
            limits=httpx.Limits(max_connections=50, max_keepalive_connections=20),
        )
        yield
    finally:
        if downstream_client is not None:
            await downstream_client.aclose()
        await repository.close()


app = FastAPI(title="RecSys Demo API", version="1.0.0", lifespan=lifespan)
configure_telemetry(app)


@app.middleware("http")
async def observe_requests(request, call_next):
    started = time.perf_counter()
    status_code = 500
    try:
        response = await call_next(request)
        status_code = response.status_code
        return response
    finally:
        route = request.scope.get("route")
        path = getattr(route, "path", request.url.path)
        REQUESTS.labels(request.method, path, str(status_code)).inc()
        LATENCY.labels(request.method, path).observe(time.perf_counter() - started)
        LOGGER.info(
            "request_complete method=%s path=%s status=%s duration_ms=%.3f",
            request.method,
            path,
            status_code,
            (time.perf_counter() - started) * 1000,
        )


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/ready")
async def ready() -> dict[str, str]:
    try:
        await repository.ping()
        assert downstream_client is not None
        inference, features = await asyncio.gather(
            downstream_client.get(f"{inference_url()}/healthz"),
            downstream_client.get(f"{feature_url()}/healthz"),
        )
        inference.raise_for_status()
        features.raise_for_status()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"dependency not ready: {exc}") from exc
    return {"status": "ready"}


@app.get("/metrics")
async def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/api/users", response_model=UserPage)
async def users(
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> UserPage:
    try:
        items, total = await repository.users(limit, offset)
    except Exception as exc:
        raise HTTPException(status_code=503, detail="database unavailable") from exc
    return UserPage(items=items, total=total, limit=limit, offset=offset)


@app.get("/api/products", response_model=ProductPage)
async def products(
    limit: int = Query(default=24, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
) -> ProductPage:
    try:
        items, total = await repository.products(limit, offset)
    except Exception as exc:
        raise HTTPException(status_code=503, detail="database unavailable") from exc
    return ProductPage(items=items, total=total, limit=limit, offset=offset)
