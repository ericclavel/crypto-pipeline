from pydantic import BaseModel, Field, field_validator
from datetime import datetime

class CryptoPriceData(BaseModel):
    asset_id: str = Field(..., min_length=1)
    price_usd: float = Field(..., gt=0)
    last_updated: datetime

    @field_validator('last_updated', mode='before')
    @classmethod
    def parse_unix_timestamp(cls, v):
        if isinstance(v, (int, float)):
            return datetime.fromtimestamp(v)
        return v
