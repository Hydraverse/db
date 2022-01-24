from typing import Optional

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
        super().__init__(
            url=conf.url,
            response_factory=BaseRPC.RESPONSE_FACTORY_JSON
        )

    def server_info(self) -> schemas.ServerInfo:
        return schemas.ServerInfo(**self.get("/server/info"))

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

    def user_del(self, user: schemas.User) -> None:
        self.post(
            f"/u/{user.uniq.pkid}",
            request_type="delete",
            **schemas.UserDelete(tg_user_id=user.tg_user_id).dict(),
        )

    def user_info_put(self, user: schemas.User, info: AttrDict, over: bool = False) -> schemas.UserInfoUpdate.Result:
        return schemas.UserInfoUpdate.Result(
            **self.post(
                f"/u/{user.uniq.pkid}/info",
                request_type="put",
                **schemas.UserInfoUpdate(info=info, over=over).dict(),
            )
        )

    def user_addr_get(self, user: schemas.User, address: str) -> Optional[schemas.UserAddr]:
        result = self.get(
            f"/u/{user.uniq.pkid}/a/{address}",
        )

        return result if result is None else schemas.UserAddr(
            **result
        )

    def user_addr_add(self, user: schemas.User, address: str) -> schemas.UserAddr:
        return schemas.UserAddr(
            **self.post(
                f"/u/{user.uniq.pkid}/a/",
                **schemas.UserAddrAdd(address=address).dict(),
            )
        )

    def user_addr_del(self, user: schemas.User, user_addr: schemas.UserAddr) -> schemas.DeleteResult:
        return schemas.DeleteResult(
            **self.post(
                f"/u/{user.uniq.pkid}/a/{user_addr.pkid}",
                request_type="delete",
            )
        )

    def user_addr_token_add(self, user_addr: schemas.UserAddr, address: str) -> schemas.UserAddrTokenAdd.Result:
        return schemas.UserAddrTokenAdd.Result(
            **self.post(
                f"/u/{user_addr.user_pk}/a/{user_addr.pkid}/t",
                **schemas.UserAddrTokenAdd(address=address).dict(),
            )
        )

    def user_addr_token_del(self, user_addr: schemas.UserAddr, address: str) -> schemas.DeleteResult:
        return schemas.DeleteResult(
            **self.post(
                f"/u/{user_addr.user_pk}/a/{user_addr.pkid}/t/{address}",
                request_type="delete",
            )
        )

    def user_addr_hist_del(self, user: schemas.User, user_addr_hist: schemas.UserAddrHist) -> schemas.DeleteResult:
        return schemas.DeleteResult(
            **self.post(
                f"/u/{user.uniq.pkid}/a/{user_addr_hist.user_addr_pk}/{user_addr_hist.addr_hist.pkid}",
                request_type="delete",
            )
        )
