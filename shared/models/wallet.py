from pydantic import BaseModel


class Wallet(BaseModel):
    address: str
