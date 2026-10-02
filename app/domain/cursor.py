from datetime import datetime, timezone

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator


class SyncCursor(BaseModel):
    model_config = ConfigDict(frozen=True)

    source_updated_at: AwareDatetime
    order_key: int = Field(gt=0, strict=True)

    @field_validator("source_updated_at")
    @classmethod
    def microsecond_precision(cls, value: datetime) -> datetime:
        # Source TIMESTAMP_TZ(6) and PostgreSQL timestamptz use microseconds.
        return value.astimezone(timezone.utc)

    def follows(self, other: "SyncCursor") -> bool:
        return (self.source_updated_at, self.order_key) > (other.source_updated_at, other.order_key)
