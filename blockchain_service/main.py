from fastapi import FastAPI

from shared import constants

app = FastAPI()


@app.get("/health")
def health():
    return {"status": "ok"}
