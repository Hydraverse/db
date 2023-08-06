import enum
import json
from typing import Iterable, Union

import sseclient

from hydb.api import schemas


class Events:
    class EventType(enum.StrEnum):
        BLOCK = "block"

    @staticmethod
    def event_is_block(event: Union[str, sseclient.Event]) -> bool:
        return (
                (event.event if isinstance(event, sseclient.Event) else event)
                == Events.EventType.BLOCK
        )

    @staticmethod
    def block_event_decode(data: Union[dict, str, sseclient.Event]) -> schemas.BlockSSEResult:
        return schemas.BlockSSEResult(
            **(
                data if isinstance(data, dict) else
                json.loads(
                    data.data if isinstance(data, sseclient.Event) else
                    data
                )
            )
        )

    @staticmethod
    def filter_block_events(sse_client: sseclient.SSEClient) -> Iterable[sseclient.Event]:
        return filter(lambda event: Events.event_is_block(event), sse_client.events())

    @staticmethod
    def yield_block_events(sse_client: sseclient.SSEClient) -> Iterable[schemas.BlockSSEResult]:
        return sorted(
            map(
                Events.block_event_decode,
                Events.filter_block_events(sse_client)
            ),
            key=lambda be: be.id
        )

