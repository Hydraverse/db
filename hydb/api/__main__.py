from typing import List

from fastapi import FastAPI, Depends, HTTPException
from ..db import DB

from .crud import models, schemas
from . import crud

app: FastAPI = FastAPI()
dbase: DB = DB()


@app.get("/")
def root():
    return {"message": "Hello, World!"}


@app.get("/hello/{name}")
def say_hello(name: str):
    return {"message": f"Hello {name}"}


@app.get("/user/{tg_user_id}", response_model=schemas.User)
def read_user(tg_user_id: int, db: DB = Depends(dbase.yield_with_session)):
    db_user = crud.get_user_by_tg_id(db, tg_user_id)

    if db_user is None:
        raise HTTPException(status_code=404, detail="User not found")

    return db_user


@app.post("/user/", response_model=schemas.User)
def create_user(user: schemas.UserCreate, db: DB = Depends(dbase.yield_with_session)):
    db_user = crud.get_user_by_tg_id(db, user.tg_user_id)

    if db_user:
        raise HTTPException(status_code=400, detail="Telegram ID already registered.")

    return crud.create_user(db=db, user=user)


# Use as template for Addrs.
# @app.get("/users/", response_model=List[schemas.User])
# def read_users(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
#     users = crud.get_users(db, skip=skip, limit=limit)
#     return users
