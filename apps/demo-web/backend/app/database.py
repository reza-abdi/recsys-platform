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


class DemoRepository:
    def __init__(self, pool: AsyncConnectionPool | None = None) -> None:
        self.pool = pool or AsyncConnectionPool(
            conninfo=(
                f"host={os.getenv('POSTGRES_HOST', 'source-postgres.recsys-dataflow.svc.cluster.local')} "
                f"port={os.getenv('POSTGRES_PORT', '5432')} "
                f"dbname={os.getenv('POSTGRES_DB', 'recsys')} "
                f"user={os.getenv('POSTGRES_USER', 'recsys')} "
                f"password={os.getenv('POSTGRES_PASSWORD', '')} "
                f"connect_timeout={os.getenv('POSTGRES_CONNECT_TIMEOUT_SECONDS', '5')}"
            ),
            min_size=max(1, int(os.getenv("POSTGRES_POOL_MIN_SIZE", "1"))),
            max_size=max(1, int(os.getenv("POSTGRES_POOL_MAX_SIZE", "8"))),
            timeout=float(os.getenv("POSTGRES_POOL_TIMEOUT_SECONDS", "5")),
            open=False,
            kwargs={"row_factory": dict_row},
        )

    async def open(self) -> None:
        await self.pool.open(
            wait=True,
            timeout=float(os.getenv("POSTGRES_STARTUP_TIMEOUT_SECONDS", "15")),
        )

    async def close(self) -> None:
        await self.pool.close()

    async def ping(self) -> None:
        async with self.pool.connection() as connection, connection.cursor() as cursor:
            await cursor.execute("SELECT 1")
            await cursor.fetchone()

    async def users(self, limit: int, offset: int) -> tuple[list[User], int]:
        async with self.pool.connection() as connection, connection.cursor() as cursor:
            await cursor.execute("SELECT count(*) AS total FROM users WHERE is_active IS TRUE")
            total = int((await cursor.fetchone())["total"])
            await cursor.execute(
                """
                SELECT user_id, segment, city
                FROM users
                WHERE is_active IS TRUE
                ORDER BY user_id
                LIMIT %s OFFSET %s
                """,
                (limit, offset),
            )
            return [User.model_validate(row) for row in await cursor.fetchall()], total

    async def products(self, limit: int, offset: int) -> tuple[list[Product], int]:
        async with self.pool.connection() as connection, connection.cursor() as cursor:
            await cursor.execute("SELECT count(*) AS total FROM products WHERE is_active IS TRUE")
            total = int((await cursor.fetchone())["total"])
            await cursor.execute(
                """
                SELECT product_id, product_name, category_id, category_code,
                       brand_id, brand_name, current_price, price_bucket
                FROM products
                WHERE is_active IS TRUE
                ORDER BY popularity_weight DESC NULLS LAST, product_id
                LIMIT %s OFFSET %s
                """,
                (limit, offset),
            )
            return [self._product(row) for row in await cursor.fetchall()], total

    async def user_exists(self, user_id: int) -> bool:
        async with self.pool.connection() as connection, connection.cursor() as cursor:
            await cursor.execute("SELECT 1 FROM users WHERE user_id=%s AND is_active IS TRUE", (user_id,))
            return await cursor.fetchone() is not None

    async def product(self, product_id: int) -> Product | None:
        async with self.pool.connection() as connection, connection.cursor() as cursor:
            await cursor.execute(
                """
                SELECT product_id, product_name, category_id, category_code,
                       brand_id, brand_name, current_price, price_bucket
                FROM products WHERE product_id=%s AND is_active IS TRUE
                """,
                (product_id,),
            )
            row = await cursor.fetchone()
            return self._product(row) if row else None

    async def products_by_id(self, product_ids: list[int]) -> dict[int, Product]:
        if not product_ids:
            return {}
        async with self.pool.connection() as connection, connection.cursor() as cursor:
            await cursor.execute(
                """
                SELECT product_id, product_name, category_id, category_code,
                       brand_id, brand_name, current_price, price_bucket
                FROM products WHERE product_id = ANY(%s)
                """,
                (product_ids,),
            )
            products = [self._product(row) for row in await cursor.fetchall()]
            return {product.product_id: product for product in products}


    @staticmethod
    def _product(row: dict[str, Any]) -> Product:
        payload = dict(row)
        payload["current_price"] = float(payload.get("current_price") or 0)
        return Product.model_validate(payload)
