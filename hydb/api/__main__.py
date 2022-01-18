from fastapi import FastAPI
from ..db import DB

app: FastAPI = FastAPI()
db: DB = DB()


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/hello/{name}")
async def say_hello(name: str):
    return {"message": f"Hello {name}"}
