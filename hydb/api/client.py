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

    def read_user(self, tg_user_id: int) -> schemas.User:
        return schemas.User(**self.get(f"/user/{tg_user_id}"))

    def create_user(self, tg_user_id: int) -> schemas.User:
        return schemas.User(
            **self.post(
                f"/user/",
                **schemas.UserCreate(tg_user_id=tg_user_id).dict()
            )
        )
