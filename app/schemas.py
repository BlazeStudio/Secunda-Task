import json
from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator

from app.models import Currency, PaymentStatus

METADATA_MAX_BYTES = 16 * 1024

Amount = Annotated[Decimal, Field(gt=0, max_digits=18, decimal_places=2)]


class PaymentCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    amount: Amount = Field(examples=["1500.00"])
    currency: Currency
    description: str = Field(min_length=1, max_length=1024, examples=["Order #1042"])
    metadata: dict[str, Any] = Field(default_factory=dict, examples=[{"order_id": 1042}])
    webhook_url: HttpUrl = Field(examples=["https://example.com/webhooks/payments"])

    @field_validator("metadata")
    @classmethod
    def _limit_metadata_size(cls, value: dict[str, Any]) -> dict[str, Any]:
        if len(json.dumps(value, ensure_ascii=False).encode()) > METADATA_MAX_BYTES:
            raise ValueError(f"metadata must not exceed {METADATA_MAX_BYTES} bytes when encoded")
        return value


class PaymentAccepted(BaseModel):
    payment_id: UUID
    status: PaymentStatus
    created_at: datetime


class PaymentRead(BaseModel):
    """Full payment representation, also used as the webhook body."""

    model_config = ConfigDict(from_attributes=True)

    payment_id: UUID = Field(validation_alias="id")
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any] = Field(validation_alias="metadata_")
    status: PaymentStatus
    idempotency_key: str
    webhook_url: str
    created_at: datetime
    processed_at: datetime | None


class PaymentEvent(BaseModel):
    payment_id: UUID
