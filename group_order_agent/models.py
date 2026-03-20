from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import List
from uuid import uuid4


def money(value: str | float | int | Decimal) -> Decimal:
    return Decimal(str(value)).quantize(Decimal("0.01"))


@dataclass(frozen=True)
class OrderItem:
    name: str
    unit_price: Decimal
    quantity: int

    @property
    def subtotal(self) -> Decimal:
        return money(self.unit_price * self.quantity)


@dataclass(frozen=True)
class OrderInput:
    user_id: str
    restaurant_id: str
    address_tag: str
    items: List[OrderItem]
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    idempotency_key: str = field(default_factory=lambda: str(uuid4()))

    @property
    def food_subtotal(self) -> Decimal:
        total = sum((item.subtotal for item in self.items), start=money(0))
        return money(total)


@dataclass
class Order:
    order_id: str
    room_id: str
    user_id: str
    restaurant_id: str
    address_tag: str
    items: List[OrderItem]
    created_at: datetime
    food_subtotal: Decimal


@dataclass
class RoomSettlement:
    room_id: str
    restaurant_id: str
    total_food: Decimal
    total_discount: Decimal
    total_delivery: Decimal
    total_service_fee: Decimal
    total_payable: Decimal
    baseline_delivery: Decimal
    saved_delivery: Decimal
    platform_revenue: Decimal
    user_payables: dict[str, Decimal]
    merchant_receivables: dict[str, Decimal]
    user_merchant_allocations: dict[str, dict[str, Decimal]]
    user_platform_allocations: dict[str, Decimal]


@dataclass
class GroupRoom:
    room_id: str
    restaurant_id: str
    address_tag: str
    open_at: datetime
    close_at: datetime
    status: str = "OPEN"
    order_ids: List[str] = field(default_factory=list)
