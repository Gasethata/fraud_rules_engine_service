from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional, List


class Token(BaseModel):
    access_token: str
    token_type: str = "bearer"

class LoginRequest(BaseModel):
    username: str = Field(..., min_length=1, max_length=50)
    password: str = Field(..., min_length=1)

class FlaggedTransaction(BaseModel):
    id: int
    transaction_id: str
    reason: List[str]
    flagged_at: datetime
    rule_ids: Optional[List[int]] = None

    class Config:
        from_attributes = True
        json_encoders = {
            datetime: lambda v: v.isoformat()  # Custom datetime encoding
        }