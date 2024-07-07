from pydantic import BaseModel, field_serializer
from typing import Optional, Any

class Transaction(BaseModel):
    hash: Optional[str]
    name: str
    quantity: Optional[float]
    age: Optional[str]

    @field_serializer('quantity')
    def round_quantity(self, value: Any):
        return round(value, 5)
