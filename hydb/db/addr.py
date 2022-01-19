from __future__ import annotations
import enum
from functools import lru_cache
from typing import Optional, Tuple
import binascii

from attrdict import AttrDict
from hydra import log
from hydra.rpc.hydra_rpc import BaseRPC
from sqlalchemy import Column, String, Enum, Integer, BigInteger
from sqlalchemy.exc import NoResultFound
from sqlalchemy.orm import relationship

from .base import *
from .db import DB
from .block import Block, TX
from .addr_tx import AddrTX
from .user_addr_tx import UserAddrTX

__all__ = "Addr", "AddrTX", "Smac", "Tokn", "NFT"


@dictattrs("pkid", "date_create", "date_update", "addr_tp", "addr_hx", "addr_hy", "block_h", "info")
class Addr(Base):
    __tablename__ = "addr"
    __table_args__ = (
        DbInfoColumnIndex(__tablename__),
    )

    class Type(enum.Enum):
        H = "HYDRA"
        S = "smart contract"
        N = "NFT"
        T = "token"

        @staticmethod
        def by_len(address: str) -> Optional[Addr.Type]:
            length = len(address)

            return (
                Addr.Type.H if length == 34 else
                Addr.Type.S if length == 40 else
                None
            )

    pkid = DbPkidColumn()
    date_create = DbDateCreateColumn()
    date_update = DbDateUpdateColumn()
    addr_tp = Column(Enum(Type, validate_strings=True), nullable=False, index=True)
    addr_hx = Column(String(40), nullable=False, unique=True, index=True)
    addr_hy = Column(String(34), nullable=False, unique=True, index=True)
    block_h = Column(Integer, nullable=True)
    info = DbInfoColumn()

    addr_users = relationship(
        "UserAddr",
        back_populates="addr",
        cascade="all, delete-orphan",
        single_parent=True,
    )

    addr_txes = relationship(
        "AddrTX",
        back_populates="addr",
        cascade="all, delete-orphan",
        single_parent=True,
    )

    addr_tokns = relationship(
        "ToknAddr",
        back_populates="addr",
        cascade="all, delete-orphan",
        single_parent=True,
    )

    __mapper_args__ = {
        "polymorphic_identity": Type.H,
        "polymorphic_on": addr_tp,
        "with_polymorphic": "*",
    }

    @staticmethod
    def __make(addr_tp: Type, addr_hx: str, addr_hy: str, **kwds) -> [Addr, Smac, Tokn, NFT]:
        if addr_tp == Addr.Type.H:
            return Addr(addr_hx=addr_hx, addr_hy=addr_hy, **kwds)
        elif addr_tp == Addr.Type.S:
            return Smac(addr_hx=addr_hx, addr_hy=addr_hy, **kwds)
        elif addr_tp == Addr.Type.T:
            return Tokn(addr_hx=addr_hx, addr_hy=addr_hy, **kwds)
        elif addr_tp == Addr.Type.N:
            return NFT(addr_hx=addr_hx, addr_hy=addr_hy, **kwds)
        else:
            log.warning(f"Unrecognized Addr.Type '{addr_tp}'")
            return Addr(addr_tp=addr_tp, addr_hx=addr_hx, addr_hy=addr_hy, **kwds)

    def __str__(self):
        return self.addr_hy

    def on_new_addr_tx(self, db: DB, addr_tx: AddrTX) -> bool:
        tx = addr_tx.tx
        if self.block_h != tx.block.height:
            self.block_h = tx.block.height
            self.on_new_block(db, tx.block)

        self.on_new_tx(db, tx)

        has_users = False
        uatxes = []

        for addr_user in self.addr_users:
            uatxes.append(UserAddrTX(user=addr_user.user, addr_tx=addr_tx))

        for uatx in uatxes:
            if uatx.on_new_addr_tx(db):
                has_users = True

        return has_users

    def on_new_block(self, db: DB, block: Block):
        self.update_balances(db, tx=None)

    def on_new_tx(self, db: DB, tx: TX):
        self.update_balances(db, tx)

    def update_balances(self, db: DB, tx: Optional[TX]):
        if tx is not None:
            return  # Only update info & balance once per new block.

        add = False
        balanc = ...

        try:
            if self.addr_tp == Addr.Type.H:
                info = db.rpcx.get_address(self.addr_hy)
            else:
                info = db.rpcx.get_contract(self.addr_hx)
                del info["addressHex"]

                # TODO: Determine why this is different on explorer and how to acquire!
                #   e.g. 09188dbfe8e915e6a3c42842b079432007a3673f
                #        -> e2vb5jpC2hodZuqRefGd7XVWPZQEbqd8uk (explorer api)
                #        -> Ta8uUv4ha1krJeDB1kcLWGR42ShiA3Fpxy (fromhexaddress)
                # del info["address"]

        except BaseRPC.Exception as exc:
            log.critical(f"Addr RPC error: {str(exc)}", exc_info=exc)
            return None

        # NOTE: If loading token balances from Explorer, keep this data.
        #       Current decision is to load directly from local blockchain db.
        #       Leaves behind any qrc721 balances.
        if "qrc20Balances" in info:
            del info["qrc20Balances"]

        if "qrc721Balances" in info:
            del info["qrc721Balances"]

        if "qrc20" in info:
            del info.qrc20.name
            del info.qrc20.symbol
            del info.qrc20.decimals
            del info.qrc20.totalSupply
            # Leaves behind 'holders', 'version', 'transactions'

        if "qrc721" in info:
            del info.qrc721

        if info != self.info:
            self.info = info
            add = True

        if add:
            db.Session.add(self)

    # noinspection PyUnusedLocal
    def __ensure_imported(self, db: DB):
        if self.addr_tp == Addr.Type.H:
            if self.addr_hy not in db.rpc.listlabels():
                log.info(f"Importing address {self.addr_hy}")
                db.rpc.importaddress(self.addr_hy, self.addr_hy)

    def __on_new_addr(self, db: DB):
        self.update_balances(db, tx=None)
        db.Session.add(self)
        db.Session.commit()
        db.Session.refresh(self)
        Block._on_new_addr(db, self)

    def _removed_user(self, db: DB):
        if not len(self.addr_users):
            for addr_tx in list(self.addr_txes):
                addr_tx._remove(db, self.addr_txes)

            for addr_tokn in list(self.addr_tokns):
                addr_tokn._remove(db, self.addr_tokns)

            log.info(f"Deleting {self.addr_tp.value} address {str(self)} with no users.")
            db.Session.delete(self)

    @staticmethod
    def get(db: DB, address: str, create=True) -> [Addr, Smac, Tokn, NFT]:
        addr_tp, addr_hx, addr_hy, addr_attr = Addr.normalize(db, address)

        try:
            if addr_tp == Addr.Type.T:
                q: Tokn = db.Session.query(Tokn).where(
                    Tokn.addr_hx == addr_hx
                )
            elif addr_tp == Addr.Type.N:
                q: NFT = db.Session.query(NFT).where(
                    NFT.addr_hx == addr_hx
                )
            elif addr_tp == Addr.Type.S:
                q: Smac = db.Session.query(Smac).where(
                    Smac.addr_hx == addr_hx
                )
            else:
                if addr_tp != Addr.Type.H:
                    log.warning(f"Unknown Addr.normalize() type '{addr_tp}'")

                q: Addr = db.Session.query(Addr).where(
                    Addr.addr_hx == addr_hx,
                    Addr.addr_tp == addr_tp
                )

            if not create:
                return q.one_or_none()

            return q.one()

        except NoResultFound:
            addr: [Addr, Smac, Tokn, NFT] = Addr.__make(addr_tp, addr_hx, addr_hy, **addr_attr)
            addr.__on_new_addr(db)
            return addr

    @staticmethod
    @lru_cache(maxsize=None)
    def validate(db: DB, address: str):
        av = db.rpc.validateaddress(address)
        return av

    @staticmethod
    @lru_cache(maxsize=None)
    def normalize(db: DB, address: str) -> Tuple[Addr.Type, str, str, AttrDict]:
        """Normalize an input address into a tuple of (Addr.Type, addr_hex, addr_hydra).
        Or raise ValueError.
        """
        addr_tp = Addr.Type.by_len(address)
        attrs = AttrDict()

        if addr_tp is None:
            raise ValueError(f"Invalid HYDRA or smart contract address '{address}' (bad length)")

        try:
            if addr_tp == Addr.Type.H:

                valid = Addr.validate(db, address)

                if not valid.isvalid:
                    raise ValueError(f"Invalid HYDRA or smart contract address '{address}' (validation failed)")

                addr_hy = valid.address
                addr_hx = db.rpc.gethexaddress(address)

            elif addr_tp == Addr.Type.S:

                try:
                    addr_hx = hex(int(address, 16))[2:].rjust(40, "0")  # ValueError on int() fail
                except ValueError:
                    raise ValueError(f"Invalid HYDRA or smart contract address '{address}' (conversion failed)")

                addr_hy = db.rpc.fromhexaddress(addr_hx)

                addr_tp, attrs = Addr.__validate_contract(db, addr_hx)

            else:
                raise ValueError(f"Invalid HYDRA or smart contract address '{address}' (bad type)")

        except BaseRPC.Exception as exc:
            log.critical(f"Addr normalize RPC error: {str(exc)}", exc_info=exc)
            raise

        return addr_tp, addr_hx, addr_hy, attrs

    @staticmethod
    def __validate_contract(db: DB, addr_hx: str) -> Tuple[Addr.Type, AttrDict]:

        sci = AttrDict()
        addr_tp = Addr.Type.S

        try:
            # Raises BaseRPC.Exception if address does not exist
            r = db.rpc.callcontract(addr_hx, Smac.ContractMethodID.name)
        except BaseRPC.Exception:
            # Safest assumption is that this is actually a HYDRA hex address
            return Addr.Type.H, sci

        if r.executionResult.excepted == "None":
            sci.name = Addr.__sc_out_str(
                r.executionResult.output[128:]
            )

        r = db.rpc.callcontract(addr_hx, Smac.ContractMethodID.symbol)

        if r.executionResult.excepted == "None":
            sci.symb = Addr.__sc_out_str(
                r.executionResult.output[128:]
            )

            r = db.rpc.callcontract(addr_hx, Smac.ContractMethodID.totalSupply)

            if r.executionResult.excepted == "None":
                sci.supt = int(r.executionResult.output, 16)

                r = db.rpc.callcontract(addr_hx, Smac.ContractMethodID.decimals)

                if r.executionResult.excepted == "None":
                    sci.deci = int(r.executionResult.output, 16)
                    addr_tp = Addr.Type.T
                else:
                    # NFT contracts have no decimals()
                    addr_tp = Addr.Type.N

        return addr_tp, sci

    @staticmethod
    def __sc_out_str(val):
        return binascii.unhexlify(val).replace(b"\x00", b"").decode("utf-8")


from .smac import Smac, Tokn, NFT
