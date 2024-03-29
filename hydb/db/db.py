from __future__ import annotations

from contextlib import contextmanager
from typing import Optional

from attrdict import AttrDict
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session, Session
from cryptography.fernet import Fernet
import asyncio

from hydra.rpc.base import BaseRPC
from hydra.rpc import HydraRPC, ExplorerRPC
from hydra import log

from hydb.util.conf import Config


@Config.defaults
class DB:
    engine = None
    _Session: scoped_session
    _in_session: Optional[Session]
    _in_session_refcount: int = 0
    rpc: HydraRPC
    api = None  # type: hydb.api.client.HyDbClient
    url: str
    wallet: str
    passphrase: str
    address: str
    privkey: str
    fernet: Fernet
    debug: bool

    CONF = AttrDict(
        url=f"postgresql://hyve:hyve@localhost/hyve",
        passphrase="changeme",
        address="(HYDRA address owned by db)",
        privkey="(Private key for above address)",
        fernet=lambda: Fernet.generate_key(),
        debug=False,
    )

    def __init__(self):
        conf = Config.get(DB)
        self.debug = conf.get("debug", False)
        self.url = conf.url
        self.wallet = conf.get("wallet", None)

        if len(conf.fernet) != 44:
            raise ValueError("DB config fernet key wrong length. Use cryptography.fernet.Fernet.generate_key().")

        self.fernet = Fernet(conf.fernet)
        del conf.fernet

        if self.wallet:
            if len(conf.passphrase) < 52:
                raise ValueError("DB config wallet passphrase is too short (< 52).")
            elif len(conf.passphrase) > 52:
                log.warning("Processing wallet passphrase as encrypted.")
                self.passphrase = str(
                    self.fernet.decrypt(
                        bytes(conf.passphrase, encoding="ascii")
                    ),
                    encoding="ascii"
                )

                if len(self.passphrase) < 52:
                    raise ValueError("Decrypted wallet passphrase too short (< 52).")
            else:
                self.passphrase = conf.passphrase

            if len(conf.privkey) > 52:
                log.warning("Processing HYDRA privkey as encrypted.")
                self.privkey = str(
                    self.fernet.decrypt(
                        bytes(conf.privkey, encoding="ascii")
                    ),
                    encoding="ascii"
                )

                if len(self.privkey) != 52:
                    raise ValueError("DB config Decrypted HYDRA privkey wrong length (not 52).")
            elif len(conf.privkey) != 52:
                raise ValueError("DB config HYDRA privkey wrong length (not 52).")

            if len(conf.address) != 34:
                raise ValueError("DB config HYDRA address or privkey not valid.")

        self.address = conf.address

        log.debug(f"db: open url='{self.url}'")
        self.engine = create_engine(self.url)
        self._Session = scoped_session(sessionmaker(
            bind=self.engine,
            expire_on_commit=False
        ))
        self._in_session = None

        Base.metadata.create_all(self.engine)
        StatBase.metadata.create_all(self.engine)

        conf_rpc = Config.get(HydraRPC)

        self.rpc = HydraRPC(url=conf_rpc.url)
        self.rpcx = ExplorerRPC(mainnet=self.rpc.mainnet)

        from hydb.api.client import HyDbClient
        self.api = HyDbClient()

        self.__init_wallet()

    def __repr__(self):
        return f"{self.__class__.__name__}(url=\"{self.url}\")"

    def __hash__(self):
        return hash(self.url + self.rpc.url)

    def __init_wallet(self):
        if self.wallet is not None:
            self.rpc.wallet = self.wallet

            if self.wallet not in self.rpc.listwallets():
                try:
                    log.info(f"Loading wallet '{self.wallet}'...")
                    self.rpc.loadwallet(self.wallet)
                    log.info(f"Wallet '{self.wallet}' loaded, unlocking...")
                    self.rpc.walletpassphrase(self.passphrase, 99999999, staking_only=False)
                    log.info(f"Wallet unlocked.")
                except BaseRPC.Exception:
                    log.warning(f"Creating wallet '{self.wallet}'...")
                    self.rpc.createwallet(self.wallet, disable_private_keys=False, blank=False)
                    log.warning(f"Wallet '{self.wallet}' created, encrypting...")
                    self.rpc.encryptwallet(self.passphrase)
                    log.warning(f"Wallet encrypted, unlocking...")
                    self.rpc.walletpassphrase(self.passphrase, 99999999, staking_only=False)
                    log.warning(f"Wallet unlocked.")
            else:
                log.info(f"Unlocking wallet '{self.wallet}'...")
                self.rpc.walletpassphrase(self.passphrase, 99999999, staking_only=False)
                log.info(f"Wallet unlocked.")

            labels = self.rpc.listlabels()

            if "hyve" not in labels or self.address not in UserUniq.label_addrs(self, labels, "hyve"):
                log.warning(f"Importing main hyve address private key for {self.address}.")
                self.rpc.importprivkey(
                    hydraprivkey=self.privkey,
                    label="hyve",
                    rescan=True
                )
                log.info("Imported main hyve address private key.")

            UserUniq.check_wallet_addrs(self)

    @property
    def session(self) -> Optional[scoped_session]:
        return self._Session if self._in_session else None

    def sessioned(self):
        with self.with_session():
            yield self

    @contextmanager
    def with_session(self):
        self._in_session_refcount += 1
        if self._in_session_refcount == 1:
            self._in_session = self._Session()

        try:
            yield self._Session
        finally:
            self._in_session_refcount -= 1
            if self._in_session_refcount == 0:
                self._Session.remove()
                self._in_session = None

    def in_session(self, fn, *args, **kwds):
        with self.with_session():
            return fn(*args, **kwds)

    async def in_session_async(self, fn, *args):
        return await DB._run_in_executor(self.in_session, fn, *args)

    @staticmethod
    async def _run_in_executor(fn, *args):
        return await asyncio.get_event_loop().run_in_executor(None, fn, *args)


from hydb.db.base import __all__ as __base_all__
from hydb.db.base import *
from hydb.db.block import __all__ as __block_all__
from hydb.db.block import *
from hydb.db.addr import __all__ as __addr_all__
from hydb.db.addr import *
from hydb.db.user import __all__ as __user_all__
from hydb.db.user import *
from hydb.db.event import __all__ as __event_all__
from hydb.db.event import *
from hydb.db.stat import __all__ as __stat_all__
from hydb.db.stat import *

__all__ = (
    ("DB",) +
    __base_all__ +
    __user_all__ +
    __addr_all__ +
    __block_all__ +
    __event_all__ +
    __stat_all__
)

# __all__ = [n for n in globals() if n[:1] != '_']
