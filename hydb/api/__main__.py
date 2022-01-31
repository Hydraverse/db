from fastapi import FastAPI, Depends, HTTPException, APIRouter, Request
from sse_starlette.sse import EventSourceResponse
from sqlalchemy import and_

from hydra import log

from ..db import DB

from hydb.event.events import EventManager

from .crud import models, schemas
from . import crud

app: FastAPI = FastAPI()
dbase: DB = DB()

log.log_set(log.INFO)


@app.get("/")
def root():
    return {"message": "Hello, World!"}


@app.get("/hello/{name}")
def say_hello(name: str):
    return {"message": f"Hello {name}"}


@app.get("/server/info", response_model=schemas.ServerInfo)
def server_info():
    return crud.server_info(db=dbase)


@app.get("/sse/block/{block_pk}/{block_ev}")
def db_notify_block(block_pk: int, block_ev: schemas.SSEBlockEvent, db: DB = Depends(dbase.yield_with_session)):
    block: models.Block = crud.block_get(db=db, block_pk=block_pk)

    if block is None:
        raise HTTPException(status_code=404, detail=f"Block #{block_pk} not found.")

    if block_ev not in (schemas.SSEBlockEvent.create, schemas.SSEBlockEvent.mature):
        # Should not be allowed to happen per schema.
        raise HTTPException(status_code=500, detail=f"Unknown block event '{block_ev}'.")

    ev: models.Event = crud.block_sse_event_add(db=db, block=block, event=block_ev)

    log.info(f"Event #{ev.pkid} added for block #{block.height}")


@app.router.get('/sse/block')
async def sse_block(request: Request, db: DB = Depends(dbase.yield_with_session)):
    event_generator = EventManager.block_event_generator(
        db=db,
        request=request
    )

    return EventSourceResponse(event_generator)


@app.router.get('/sse/block/next')
async def sse_block_next(request: Request, db: DB = Depends(dbase.yield_with_session)):
    event_generator = EventManager.block_event_generator(
        db=db,
        request=request,
        limit=1
    )

    return EventSourceResponse(event_generator)


@app.post("/u/", response_model=schemas.User)
def user_add(user_create: schemas.UserCreate, db: DB = Depends(dbase.yield_with_session)):
    db_user = crud.user_get_by_tgid(db, user_create.tg_user_id)

    if db_user:
        raise HTTPException(status_code=400, detail="Telegram ID already registered.")

    return crud.user_add(db=db, user_create=user_create)


@app.delete("/u/{user_pk}")
def user_del(user_pk: int, user_delete: schemas.UserDelete, db: DB = Depends(dbase.yield_with_session)):
    db_user = crud.user_get_by_pkid(db, user_pk)

    if db_user is None:
        raise HTTPException(status_code=404, detail="User not found.")

    if user_delete.tg_user_id != db_user.tg_user_id:
        raise HTTPException(status_code=403, detail="TG ID Mismatch.")

    db_user.delete(db)


@app.get("/u/{user_pk}", response_model=schemas.User)
def user_get(user_pk: int, db: DB = Depends(dbase.yield_with_session)):
    db_user = crud.user_get_by_pkid(db, user_pk)

    if db_user is None:
        raise HTTPException(status_code=404, detail="User not found.")

    return db_user


@app.get("/u/tg/{tg_user_id}", response_model=schemas.User)
def user_get_tg(tg_user_id: int, db: DB = Depends(dbase.yield_with_session)):
    db_user = crud.user_get_by_tgid(db, tg_user_id)

    if db_user is None:
        raise HTTPException(status_code=404, detail="User not found.")

    return db_user


@app.put("/u/{user_pk}/info", response_model=schemas.UserInfoUpdate.Result)
def user_info_put(user_pk: int, user_info_update: schemas.UserInfoUpdate, db: DB = Depends(dbase.yield_with_session)):
    db_user = crud.user_get_by_pkid(db, user_pk)

    if db_user is None:
        raise HTTPException(status_code=404, detail="User not found.")

    return crud.user_info_update(
        db=db,
        user=db_user,
        update=user_info_update,
    )


@app.get("/u/{user_pk}/a/{address}", response_model=schemas.UserAddrFull)
def user_addr_get(user_pk: int, address: str, db: DB = Depends(dbase.yield_with_session)):
    db_user = crud.user_get_by_pkid(db, user_pk)

    if db_user is None:
        raise HTTPException(status_code=404, detail="User not found.")

    return crud.user_addr_get(db=db, user=db_user, address=address)


@app.post("/u/{user_pk}/a/", response_model=schemas.UserAddr)
def user_addr_add(user_pk: int, addr_add: schemas.UserAddrAdd, db: DB = Depends(dbase.yield_with_session)):
    db_user = crud.user_get_by_pkid(db, user_pk)

    if db_user is None:
        raise HTTPException(status_code=404, detail="User not found.")

    return crud.user_addr_add(db=db, user=db_user, addr_add=addr_add)


@app.patch("/u/{user_pk}/a/{user_addr_pk}", response_model=schemas.UserAddrUpdate.Result)
def user_addr_upd(user_pk: int, user_addr_pk: int, user_addr_update: schemas.UserAddrUpdate, db: DB = Depends(dbase.yield_with_session)):
    user_addr: models.UserAddr = db.Session.query(
        models.UserAddr
    ).where(
        and_(
            models.UserAddr.user_pk == user_pk,
            models.UserAddr.pkid == user_addr_pk,
            )
    ).one_or_none()

    if user_addr is None:
        raise HTTPException(status_code=404, detail="UserAddr not found.")

    return crud.user_addr_update(
        db=db,
        user_addr=user_addr,
        addr_update=user_addr_update
    )


@app.delete("/u/{user_pk}/a/{user_addr_pk}", response_model=schemas.DeleteResult)
def user_addr_del(user_pk: int, user_addr_pk: int, db: DB = Depends(dbase.yield_with_session)):
    db_user = crud.user_get_by_pkid(db, user_pk)

    if db_user is None:
        raise HTTPException(status_code=404, detail="User not found.")

    return crud.user_addr_del(db=db, user=db_user, user_addr_pk=user_addr_pk)


@app.post("/u/{user_pk}/a/{user_addr_pk}/t", response_model=schemas.UserAddrTokenAdd.Result)
def user_addr_token_add(user_pk: int, user_addr_pk: int, addr_token_add: schemas.UserAddrTokenAdd, db: DB = Depends(dbase.yield_with_session)):
    user_addr: models.UserAddr = db.Session.query(
        models.UserAddr
    ).where(
        and_(
            models.UserAddr.user_pk == user_pk,
            models.UserAddr.pkid == user_addr_pk,
        )
    ).one_or_none()

    if user_addr is None:
        raise HTTPException(status_code=404, detail="UserAddr not found.")

    return crud.user_addr_token_add(db=db, user_addr=user_addr, addr_token_add=addr_token_add)


@app.delete("/u/{user_pk}/a/{user_addr_pk}/t/{address}", response_model=schemas.DeleteResult)
def user_addr_token_del(user_pk: int, user_addr_pk: int, address: str, db: DB = Depends(dbase.yield_with_session)):
    user_addr: models.UserAddr = db.Session.query(
        models.UserAddr
    ).where(
        and_(
            models.UserAddr.user_pk == user_pk,
            models.UserAddr.pkid == user_addr_pk,
        )
    ).one_or_none()

    if user_addr is None:
        raise HTTPException(status_code=404, detail="UserAddr not found.")

    return crud.user_addr_token_del(db=db, user_addr=user_addr, address=address)


# Use as template for Addrs.
# @app.get("/users/", response_model=List[schemas.User])
# def read_users(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
#     users = crud.get_users(db, skip=skip, limit=limit)
#     return users
