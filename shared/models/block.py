from typing import List

from pydantic import BaseModel

from .transaction import Transaction


class Block(BaseModel):
    index: int
    timestamp: float
    transactions: List[Transaction]
    previous_hash: str
    nonce: int
    hash: str
