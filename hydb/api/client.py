from hydra.rpc.base import BaseRPC
from ..util.conf import Config, AttrDict
from . import schemas


@Config.defaults
class HyDbClient(BaseRPC):
    CONF = AttrDict(
        url="http://127.0.0.1:8000"
    )

    def __init__(self):
        conf = Config.get(HyDbClient, defaults=True, save_defaults=True)
        super().__init__(url=conf.url)

    def user_get(self, tg_user_id: int) -> schemas.User:
        return schemas.User(**self.get(f"/user/{tg_user_id}"))

    def user_add(self, tg_user_id: int) -> schemas.User:
        return schemas.User(
            **self.post(
                f"/user/",
                **schemas.UserCreate(tg_user_id=tg_user_id).dict()
            )
        )

    def user_del(self, user_pk: int, tg_user_id: int) -> None:
        self.post(
            f"/user/{tg_user_id}",
            **schemas.UserDelete(pkid=user_pk).dict()
        )

    def user_addr_add(self, user: schemas.User, address: str) -> schemas.UserAddr:
        return schemas.UserAddr(
            **self.post(
                f"/user/{user.tg_user_id}/addr/add",
                **schemas.UserAddrAdd(address=address).dict()
            )
        )

    def user_addr_del(self, user: schemas.User, addr_pk: int) -> bool:
        return self.post(
                f"/user/{user.tg_user_id}/addr/del",
                **schemas.UserAddrDel(addr_pk=addr_pk).dict()
            )
