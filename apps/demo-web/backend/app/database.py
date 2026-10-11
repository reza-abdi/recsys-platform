from __future__ import annotations

import hashlib
import json
import os
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from app.models import EventRequest, Product, RecommendationItem, User


class RecordNotFoundError(Exception):
    pass


class IdempotencyConflictError(Exception):
    pass


def utc_now() -> datetime:
    return datetime.now(UTC)


def event_type_id(action: str) -> int:
    return {"view": 1, "cart": 2, "purchase": 3}[action]


def canonical_payload_hash(request: EventRequest) -> str:
    payload = request.model_dump(mode="json", exclude={"idempotency_key"})
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def event_id_for(request: EventRequest, header_key: str | None = None) -> str:
    key = header_key or request.idempotency_key
    if not key:
        return str(uuid.uuid4())
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"https://recsys-mlops.site/events/{key}"))
