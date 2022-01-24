from asyncio import Semaphore
import json

BLOCKS = []
BLOCK_NOTIFY = Semaphore(value=0)


def block_event_notify(block_pk: int):
    BLOCKS.append(block_pk)
    BLOCK_NOTIFY.release()


async def block_event_generator(request):
    while True:
        if await request.is_disconnected():
            break

        if not await BLOCK_NOTIFY.acquire():
            continue

        block_pk = BLOCKS.pop(0)

        yield {
            "event": "block",
            "retry": 30000,
            "data": json.dumps(block_pk)
        }
