"""Hydra Bot Application.
"""
from argparse import ArgumentParser

from hydra.app import HydraApp
from hydra.rpc import HydraRPC
from hydra.test import Test

from .util.conf import Config
from . import VERSION


@HydraApp.register(name="hydb", desc="Hydraverse DB", version=VERSION)
class HyDB(HydraApp):
    db: DB = None

    @staticmethod
    def parser(parser: ArgumentParser):
        parser.add_argument("-s", "--shell", action="store_true", help="Drop to an interactive shell with DB and RPC access.")

    def render_item(self, name: str, item):
        return self.render(result=HydraRPC.Result({name: item}), name=name)

    def run(self):
        if not Config.exists():
            self.render_item("error", f"Default config created and needs editing at: {Config.APP_CONF}")
            Config.read(create=True)
            exit(-1)

        self.db = DB(self.rpc)

        if self.args.shell:
            return self.shell()

    # noinspection PyMethodMayBeStatic,PyUnresolvedReferences,PyBroadException
    def shell(self):
        import sys, traceback, code
        from hybot.data import DB, Addr, Smac, Tokn, TX, AddrTX, User, UserAddr, Block
        code.interact(local=locals())
        exit(0)


@Test.register()
class HyDBTest(Test):

    def test_0_hydb_runnable(self):
        self.assertHydraAppIsRunnable(HyDB, "-h")

    def test_1_hydb_run_default(self):
        self.assertHydraAppIsRunnable(HyDB)
