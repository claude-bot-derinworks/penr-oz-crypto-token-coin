from time import time

from pydantic import BaseModel, Field


class Transaction(BaseModel):
    sender: str
    receiver: str
    amount: float
    timestamp: float = Field(default_factory=time)
