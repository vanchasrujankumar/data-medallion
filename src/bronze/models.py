from typing import Any, Optional
from datetime import datetime
from enum import Enum
from uuid import uuid4

from pydantic import BaseModel, Field


class EventType(str, Enum):
    page_view = "page_view"
    click = "click"
    purchase = "purchase"
    signup = "signup"
    error = "error"


class Event(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid4()))
    event_type: EventType
    user_id: str
    session_id: str
    page: str
    referrer: Optional[str] = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class OrderItem(BaseModel):
    product_id: str
    product_name: str
    quantity: int = Field(gt=0)
    unit_price: float = Field(gt=0)


class Order(BaseModel):
    order_id: str = Field(default_factory=lambda: str(uuid4()))
    user_id: str
    items: list[OrderItem]
    total_amount: float = Field(gt=0)
    currency: str = "USD"
    status: str = "pending"
    shipping_address: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
