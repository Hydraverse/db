"""Hydra Bot Application.
"""
import os
from argparse import ArgumentParser

from hydra.app import HydraApp
from hydra.rpc import HydraRPC
from hydra.test import Test

from .util.conf import Config
from . import VERSION

from .db import DB, Block

os.environ["HYPY_NO_RPC_ARGS"] = "1"
# os.environ["HYPY_NO_JSON_ARGS"] = "1"


@HydraApp.register(name="hydb", desc="Hydraverse DB", version=VERSION)
class HyDb(HydraApp):
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

        self.db = DB()
        self.rpc = self.db.rpc

        if self.args.shell:
            return self.shell()

        return Block.update_task(self.db)

    # noinspection PyUnresolvedReferences,PyMethodMayBeStatic
    def shell(self):
        import sys, traceback, code
        from hydra.rpc.base import BaseRPC
        from hydb import db as models
        from hydb.api.client import HyDbClient, schemas
        rpc = self.rpc
        xrpc = self.db.rpcx
        db = self.db
        client = HyDbClient()
        code.interact(
            banner=f"Hydra DB Shell:\n  db   = {db}\n  rpc  = {rpc}\n  xrpc = {xrpc}\n",
            exitmsg="",
            local=locals(),
        )
        exit(0)


@Test.register()
class HyDbTest(Test):

    def test_0_hydb_runnable(self):
        self.assertHydraAppIsRunnable(HyDb, "-h")

