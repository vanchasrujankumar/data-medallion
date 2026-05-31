from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import date

import duckdb
from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from gold.analytics import AnalyticsEngine
from gold.config import settings
from gold.models import (
    AnomalyResponse,
    AnomalyRow,
    DAUResponse,
    DAURow,
    FunnelResponse,
    FunnelStep,
    SegmentReport,
    TopProduct,
    TopProductsResponse,
    UserSegment,
)


class AppState:
    def __init__(self) -> None:
        self.conn: duckdb.DuckDBPyConnection | None = None


state = AppState()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    state.conn = duckdb.connect(settings.duckdb_path, read_only=True)
    yield
    if state.conn is not None:
        state.conn.close()


app = FastAPI(
    title="Data Medallion API",
    description="Gold layer analytics API for the data medallion pipeline",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_engine() -> AnalyticsEngine:
    if state.conn is None:
        raise HTTPException(status_code=503, detail="Database not available")
    return AnalyticsEngine(state.conn)


@app.get("/health")
async def health() -> dict[str, str]:
    if state.conn is None:
        raise HTTPException(status_code=503, detail="Database not connected")
    return {"status": "healthy"}


@app.get("/api/v1/kpis/dau", response_model=DAUResponse)
async def get_dau(
    date_from: date = Query(...),
    date_to: date = Query(...),
    engine: AnalyticsEngine = Depends(get_engine),
) -> DAUResponse:
    try:
        df = engine.daily_active_users(date_from, date_to)
        rows = [DAURow(**row) for row in df.to_dicts()]
        total = sum(r.active_users for r in rows)
        return DAUResponse(
            data=rows,
            total_active_users=total,
            date_from=date_from,
            date_to=date_to,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/v1/products/top", response_model=TopProductsResponse)
async def get_top_products(
    n: int = Query(10, ge=1, le=100),
    engine: AnalyticsEngine = Depends(get_engine),
) -> TopProductsResponse:
    try:
        df = engine.top_products(n)
        products = [TopProduct(**row) for row in df.to_dicts()]
        return TopProductsResponse(products=products, total=len(products))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/v1/users/segments", response_model=SegmentReport)
async def get_user_segments(
    engine: AnalyticsEngine = Depends(get_engine),
) -> SegmentReport:
    try:
        df = engine.user_segment_report()
        segments = [UserSegment(**row) for row in df.to_dicts()]
        total = sum(s.user_count for s in segments)
        return SegmentReport(segments=segments, total_users=total)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/v1/funnel/conversion", response_model=FunnelResponse)
async def get_conversion_funnel(
    date_from: date = Query(...),
    date_to: date = Query(...),
    engine: AnalyticsEngine = Depends(get_engine),
) -> FunnelResponse:
    try:
        df = engine.conversion_funnel(date_from, date_to)
        funnel = [FunnelStep(**row) for row in df.to_dicts()]
        return FunnelResponse(funnel=funnel, date_from=date_from, date_to=date_to)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/v1/anomalies", response_model=AnomalyResponse)
async def get_anomalies(
    metric: str = Query("dau"),
    lookback: int = Query(30, ge=1, le=365),
    engine: AnalyticsEngine = Depends(get_engine),
) -> AnomalyResponse:
    try:
        df = engine.anomaly_detection(metric, lookback)
        rows = [AnomalyRow(**row) for row in df.to_dicts()]
        total = sum(1 for r in rows if r.is_anomaly)
        return AnomalyResponse(
            anomalies=rows,
            metric=metric,
            lookback_days=lookback,
            total_anomalies=total,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
