import asyncio
from typing import Optional, Callable

import sseclient
from sseclient import SSEClient

from hydra.rpc.base import BaseRPC
from hydb.util.conf import Config, AttrDict
from hydb.event import Events

from . import schemas


@Config.defaults
class HyDbClient(BaseRPC):

    CONF = AttrDict(
        url="http://127.0.0.1:8000"
    )

    def __init__(self):
        conf = Config.get(HyDbClient, defaults=True, save_defaults=True)
        super().__init__(
            url=conf.url,
            response_factory=BaseRPC.RESPONSE_FACTORY_JSON
        )

    def _sse_client(self, path: str) -> SSEClient:
        rsp = self.request(
            request_type="get",
            path=path,
            response_factory=BaseRPC.RESPONSE_FACTORY_RESP,
            stream=True,
            headers={
                "Accept": "text/event-stream",
            },
        )

        return SSEClient(rsp)

    def _sse_get(self, path: str, callback_fn: Callable[[sseclient.Event], None]):
        sse_client = self._sse_client(path)

        try:
            for event in sse_client.events():
                callback_fn(event)
        finally:
            sse_client.close()

    def server_info(self) -> schemas.ServerInfo:
        return schemas.ServerInfo(**self.get("/server/info"))

    def stats(self) -> schemas.Stats:
        return schemas.Stats(**self.get("/stats"))

    def sse_block_notify_create(self, block_pk: int) -> None:
        self.get(
            path=f"/sse/block/{block_pk}/{schemas.SSEBlockEvent.create}",
            response_factory=lambda rsp: None
        )

    def sse_block_notify_mature(self, block_pk: int) -> None:
        self.get(
            path=f"/sse/block/{block_pk}/{schemas.SSEBlockEvent.mature}",
            response_factory=lambda rsp: None
        )

    def sse_block_next(self) -> schemas.BlockSSEResult:
        sse_client = self._sse_client(path="/sse/block/next")

        try:
            for block_sse_result in Events.yield_block_events(sse_client):
                return block_sse_result
        finally:
            sse_client.close()

    def sse_block(self, callback_fn: Callable[[schemas.BlockSSEResult], None]):
        def callback(event: sseclient.Event):
            if Events.event_is_block(event):
                block_sse_result = Events.block_event_decode(event)
                return callback_fn(block_sse_result)

        return self._sse_get(
            path="/sse/block",
            callback_fn=callback
        )

    async def sse_block_async(self, callback_fn: Callable, loop: asyncio.AbstractEventLoop):
        def callback(event: sseclient.Event):
            if Events.event_is_block(event):
                block_sse_result = Events.block_event_decode(event)
                t = loop.create_task(callback_fn(block_sse_result))

                if not loop.is_running():
                    loop.run_until_complete(t)

        return await self.asyncc._sse_get(
            path="/sse/block",
            callback_fn=callback
        )

    def user_get(self, user_pk: int) -> schemas.User:
        return schemas.User(**self.get(f"/u/{user_pk}"))

    def user_get_tg(self, tg_user_id: int) -> schemas.User:
        return schemas.User(**self.get(f"/u/tg/{tg_user_id}"))

    def user_add(self, tg_user_id: int) -> schemas.User:
        return schemas.User(
            **self.post(
                f"/u/",
                **schemas.UserCreate(tg_user_id=tg_user_id).dict(),
            )
        )

    def user_del(self, user: schemas.UserBase) -> None:
        self.post(
            f"/u/{user.uniq.pkid}",
            request_type="delete",
            **schemas.UserDelete(tg_user_id=user.tg_user_id).dict(),
        )

    def user_info_put(self, user: schemas.UserBase, info: AttrDict, over: bool = False) -> schemas.UserInfoUpdate.Result:
        return schemas.UserInfoUpdate.Result(
            **self.post(
                f"/u/{user.uniq.pkid}/info",
                request_type="put",
                **schemas.UserInfoUpdate(info=info, over=over).dict(),
            )
        )

    def user_addr_get(self, user: schemas.UserBase, address: str) -> Optional[schemas.UserAddrFull]:
        result = self.get(
            f"/u/{user.uniq.pkid}/a/{address}",
        )

        return result if result is None else schemas.UserAddr(
            **result
        )

    def user_addr_add(self, user: schemas.UserBase, address: str, name: Optional[str] = None) -> schemas.UserAddr:
        return schemas.UserAddr(
            **self.post(
                f"/u/{user.uniq.pkid}/a/",
                **schemas.UserAddrAdd(
                    address=address,
                    name=name
                ).dict(),
            )
        )

    def user_addr_upd(self, user_addr: schemas.UserAddrBase, addr_update: schemas.UserAddrUpdate) -> schemas.UserAddrUpdate.Result:
        return schemas.UserAddrUpdate.Result(
            **self.post(
                f"/u/{user_addr.user_pk}/a/{user_addr.pkid}",
                request_type="patch",
                **addr_update.dict()
            )
        )

    def user_addr_del(self, user_addr: schemas.UserAddrBase) -> schemas.DeleteResult:
        return schemas.DeleteResult(
            **self.post(
                f"/u/{user_addr.user_pk}/a/{user_addr.pkid}",
                request_type="delete",
            )
        )

    def user_addr_token_add(self, user_addr: schemas.UserAddrBase, address: str) -> schemas.UserAddrTokenAdd.Result:
        return schemas.UserAddrTokenAdd.Result(
            **self.post(
                f"/u/{user_addr.user_pk}/a/{user_addr.pkid}/t",
                **schemas.UserAddrTokenAdd(address=address).dict(),
            )
        )

    def user_addr_token_del(self, user_addr: schemas.UserAddrBase, address: str) -> schemas.DeleteResult:
        return schemas.DeleteResult(
            **self.post(
                f"/u/{user_addr.user_pk}/a/{user_addr.pkid}/t/{address}",
                request_type="delete",
            )
        )
