"""Status display.
"""
from attrdict import AttrDict
from hydra.app.top import HydraApp, TopApp

from hydb.api.client import HyDbClient


@HydraApp.register(name="stat", desc="Show status periodically", version="1.0")
class StatApp(TopApp):
    api: HyDbClient

    def setup(self):
        self.api = HyDbClient()

        super().setup()

    def read(self):
        return AttrDict(self.api.stats().dict())

    # noinspection PyShadowingBuiltins
    def display(self, print=print):
        self.render(self.read(), name="stat", print_fn=print, ljust=self.ljust)


if __name__ == "__main__":
    StatApp.main()
