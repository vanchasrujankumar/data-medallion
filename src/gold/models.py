from datetime import date

from pydantic import BaseModel


class DAURow(BaseModel):
    event_date: date
    active_users: int
    total_events: int
    avg_conversion_rate: float


class DAUResponse(BaseModel):
    data: list[DAURow]
    total_active_users: int
    date_from: date
    date_to: date


class TopProduct(BaseModel):
    product_id: str
    product_name: str
    total_revenue: float
    order_count: int


class TopProductsResponse(BaseModel):
    products: list[TopProduct]
    total: int


class UserSegment(BaseModel):
    segment_name: str
    user_count: int
    avg_spend: float
    avg_recency_days: int


class SegmentReport(BaseModel):
    segments: list[UserSegment]
    total_users: int


class FunnelStep(BaseModel):
    step_name: str
    users: int
    count: int


class FunnelResponse(BaseModel):
    funnel: list[FunnelStep]
    date_from: date
    date_to: date


class AnomalyRow(BaseModel):
    metric: str
    date: date
    value: float
    z_score: float
    is_anomaly: bool


class AnomalyResponse(BaseModel):
    anomalies: list[AnomalyRow]
    metric: str
    lookback_days: int
    total_anomalies: int
