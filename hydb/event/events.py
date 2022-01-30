import enum
from asyncio import Semaphore
from typing import Optional

from fastapi import Request

from hydra import log

from hydb.db import DB
import hydb.db as models
from hydb.api import schemas

from . import Events


class EventManager:

    class BlockLoader:
        claimant: str
        db: DB
        sem: Semaphore

        def __init__(self, db: DB, claimant: str):
            self.db = db
            self.claimant = claimant
            self.sem = Semaphore(value=1)

        def __event_insert_callback(self, _, target: models.Event):
            if Events.event_is_block(target.event):
                self.sem.release()

        def __enter__(self):
            models.Event.insert_listener_add(self.__event_insert_callback)
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            models.Event.insert_listener_rem(self.__event_insert_callback)

        async def wait(self):
            await self.sem.acquire()

        async def next(self) -> Optional[schemas.BlockSSEResult]:
            results = await self.db.in_session_async(self.__load_block_events)

            return results[0] if len(results) else None

        def __load_block_events(self):
            return [
                Events.block_event_decode(event.data)
                for event in
                models.Event.claim_all_for(
                    db=self.db,
                    event=Events.EventType.BLOCK,
                    claimant=self.claimant,
                    limit=1
                )
            ]

    @staticmethod
    async def block_event_generator(db: DB, request: Request, limit: Optional[int] = None):
        claimant = request.client.host
        sent = 0

        with EventManager.BlockLoader(db=db, claimant=claimant) as block_loader:
            while 1:
                if await request.is_disconnected():
                    break

                await block_loader.wait()

                while 1:
                    block_sse_result = await block_loader.next()

                    if block_sse_result is None:
                        break

                    yield {
                        "event": Events.EventType.BLOCK,
                        "retry": 30000,
                        "data": block_sse_result.json(encoder=str)
                    }

                    sent += 1

                    log.info(
                        f"Sent {block_sse_result.event} event for block #{block_sse_result.block.height} to {claimant}."
                    )

                    if limit is not None and sent >= limit:
                        return

