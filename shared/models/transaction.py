from time import time

from pydantic import BaseModel


class Transaction(BaseModel):
    sender: str
    receiver: str
    amount: float
    timestamp: float = time()
