from pydantic import BaseModel, field_serializer
from typing import Optional, Any

class Transaction(BaseModel):
    hash: Optional[str] = None
    name: str
    quantity: Optional[float] = None
    age: Optional[int] = None
    near_amount: Optional[float] = None
    hot_amount: Optional[float] = None
    claim_period: Optional[int] = None

