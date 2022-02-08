"""Status display.
"""
from attrdict import AttrDict
from hydra.app.top import HydraApp, TopApp

from hydb.api.crud import stats_get
from hydb.db import DB


@HydraApp.register(name="stat", desc="Show status periodically", version="1.0")
class StatApp(TopApp):
    db: DB

    def setup(self):
        self.db = DB()

        super().setup()

    def read(self):
        return AttrDict(stats_get(self.db).dict())

    # noinspection PyShadowingBuiltins
    def display(self, print=print):
        self.render(self.read(), name="stat", print_fn=print, ljust=self.ljust)


if __name__ == "__main__":
    StatApp.main()
