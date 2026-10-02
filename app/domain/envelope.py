import hashlib
import json
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Annotated, Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_serializer, field_validator, model_validator

PositiveInt = Annotated[int, Field(strict=True, gt=0, le=9223372036854775807)]


def utc_timestamp(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must have an explicit timezone")
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def stable_event_id(source: str, entity_key: str, source_version: int) -> str:
    identity = json.dumps([source, entity_key, source_version], separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()


class OrderPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False, frozen=True)

    order_key: PositiveInt
    customer_key: PositiveInt
    customer_name: str = Field(min_length=1, max_length=200)
    order_status: Literal["O", "F", "P"]
    total_price: Decimal = Field(ge=0, max_digits=18, decimal_places=2)
    order_date: date

    @field_validator("total_price", mode="before")
    @classmethod
    def reject_float_money(cls, value: object) -> object:
        if isinstance(value, (float, bool)):
            raise ValueError("money must be a decimal string or Decimal, never a binary float")
        return value

    @field_serializer("total_price")
    def serialize_money(self, value: Decimal) -> str:
        return format(value.quantize(Decimal("0.01")), ".2f")


class EventEnvelope(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    event_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    event_type: Literal["insert", "update"]
    source: str = Field(min_length=1, max_length=200, pattern=r"^\S+$")
    entity_key: str = Field(pattern=r"^order:[1-9][0-9]*$")
    payload: OrderPayload
    occurred_at: AwareDatetime
    captured_at: AwareDatetime
    schema_version: Literal[1] = 1
    source_version: PositiveInt
    source_updated_at: AwareDatetime
    batch_id: UUID
    sync_run_id: UUID
    batch_sequence: PositiveInt

    @field_validator("occurred_at", "captured_at", "source_updated_at", mode="before")
    @classmethod
    def explicit_timezone_input(cls, value: object) -> object:
        if not isinstance(value, (str, datetime)):
            raise ValueError("timestamps require a timezone-aware datetime or RFC3339 string")
        return value

    @field_validator("occurred_at", "captured_at", "source_updated_at")
    @classmethod
    def normalize_utc(cls, value: datetime) -> datetime:
        return value.astimezone(timezone.utc)

    @field_validator("schema_version", mode="before")
    @classmethod
    def strict_schema_version(cls, value: object) -> object:
        if type(value) is not int:
            raise ValueError("schema_version must be an integer")
        return value

    @field_serializer("occurred_at", "captured_at", "source_updated_at")
    def serialize_timestamp(self, value: datetime) -> str:
        return utc_timestamp(value)

    @model_validator(mode="after")
    def validate_identity(self) -> "EventEnvelope":
        if self.entity_key != f"order:{self.payload.order_key}":
            raise ValueError("entity_key does not match order_key")
        if self.occurred_at != self.source_updated_at:
            raise ValueError("occurred_at must equal source_updated_at")
        if self.event_id != stable_event_id(self.source, self.entity_key, self.source_version):
            raise ValueError("event_id does not match the source entity version")
        return self

    @property
    def partition_key(self) -> str:
        return self.entity_key

    def content_hash(self) -> str:
        """Detect mutation of the same source version; exclude delivery metadata."""
        content = self.model_dump(mode="json", exclude={"captured_at", "batch_id", "sync_run_id", "batch_sequence"})
        return hashlib.sha256(json.dumps(content, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
