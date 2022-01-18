from hydra.rpc.base import BaseRPC
from hydb.util.conf import Config, AttrDict
from hydb.api import schemas


@Config.defaults
class HyDbRPC(BaseRPC):
    CONF = AttrDict(
        url="http://127.0.0.1:8000"
    )

    def __init__(self):
        conf = Config.get(HyDbRPC, defaults=True, save_defaults=True)
        super().__init__(url=conf.url)

    def read_user(self, tg_user_id: int) -> schemas.User:
        return schemas.User(**self.get(f"/user/{tg_user_id}"))

