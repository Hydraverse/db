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


@app.post("/user/", response_model=schemas.User)
def user_add(user: schemas.UserCreate, db: DB = Depends(dbase.yield_with_session)):
    db_user = crud.user_get(db, user.tg_user_id)

    if db_user:
        raise HTTPException(status_code=400, detail="Telegram ID already registered.")

    return crud.user_add(db=db, user=user)


@app.post("/user/{tg_user_id}")
def user_del(tg_user_id: int, user: schemas.UserDelete, db: DB = Depends(dbase.yield_with_session)):
    db_user = crud.user_get(db, tg_user_id)

    if db_user is None:
        raise HTTPException(status_code=404, detail="User not found.")

    if user.pkid != db_user.pkid:
        raise HTTPException(status_code=403, detail="PKID Mismatch.")

    db_user.delete(db)


@app.get("/user/{tg_user_id}", response_model=schemas.User)
def user_read(tg_user_id: int, db: DB = Depends(dbase.yield_with_session)):
    db_user = crud.user_get(db, tg_user_id)

    if db_user is None:
        raise HTTPException(status_code=404, detail="User not found.")

    return db_user


@app.post("/user/{tg_user_id}/addr/add", response_model=schemas.UserAddr)
def user_addr_add(tg_user_id: int, addr: schemas.UserAddrAdd, db: DB = Depends(dbase.yield_with_session)):
    db_user = crud.user_get(db, tg_user_id)

    if db_user is None:
        raise HTTPException(status_code=404, detail="User not found.")

    return crud.user_addr_add(db, db_user, addr)


@app.post("/user/{tg_user_id}/addr/del", response_model=bool)
def user_addr_add(tg_user_id: int, addr: schemas.UserAddrDel, db: DB = Depends(dbase.yield_with_session)):
    db_user = crud.user_get(db, tg_user_id)

    if db_user is None:
        raise HTTPException(status_code=404, detail="User not found.")

    return crud.user_addr_del(db, db_user, addr)


# Use as template for Addrs.
# @app.get("/users/", response_model=List[schemas.User])
# def read_users(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
#     users = crud.get_users(db, skip=skip, limit=limit)
#     return users
