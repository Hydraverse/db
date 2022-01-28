from asyncio import Semaphore
import json
from typing import Iterable

import sseclient

from . import schemas

BLOCKS = []
BLOCK_NOTIFY = Semaphore(value=0)


def block_event_notify(block_sse_result: schemas.BlockSSEResult):
    BLOCKS.append(block_sse_result)
    BLOCK_NOTIFY.release()


async def block_event_generator(request):
    while True:
        if await request.is_disconnected():
            break

        if not await BLOCK_NOTIFY.acquire():
            continue

        block_sse_result: schemas.BlockSSEResult = BLOCKS.pop(0)

        yield {
            "event": "block",
            "retry": 30000,
            "data": json.dumps(block_sse_result.dict(), default=str)
        }


def event_is_block(event: sseclient.Event):
    return event.event == "block"


def block_event_decode(event: sseclient.Event):
    return schemas.BlockSSEResult(
        **json.loads(event.data)
    )


def filter_block_events(sse_client: sseclient.SSEClient) -> Iterable[sseclient.Event]:
    return filter(lambda event: event_is_block(event), sse_client.events())


def yield_block_events(sse_client: sseclient.SSEClient) -> Iterable[schemas.BlockSSEResult]:
    return map(
        block_event_decode,
        filter_block_events(sse_client)
    )
