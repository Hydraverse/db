from __future__ import annotations
import enum
from functools import lru_cache
from typing import Optional, Tuple
import binascii

from hydra import log
from hydra.app.call import Call
from hydra.rpc.hydra_rpc import BaseRPC
from sqlalchemy import Column, String, Enum, Integer
from sqlalchemy.exc import NoResultFound
from sqlalchemy.orm import relationship

from .base import *
from .db import DB
from .block import Block
from .addr_hist import AddrHist

__all__ = "Addr", "AddrHist"


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

    class ContractMethodID:
        name = Call.method_id_from_sig("name()")
        symbol = Call.method_id_from_sig("symbol()")
        decimals = Call.method_id_from_sig("decimals()")
        totalSupply = Call.method_id_from_sig("totalSupply()")
        balanceOf = Call.method_id_from_sig("balanceOf(address)")
        supportsInterface = Call.method_id_from_sig("supportsInterface(bytes4)")
        tokenURI = Call.method_id_from_sig("tokenURI(uint256)")
        ownerOf = Call.method_id_from_sig("ownerOf(uint256)")
        tokenOfOwnerByIndex = Call.method_id_from_sig("tokenOfOwnerByIndex(address,uint256)")
        tokenByIndex = Call.method_id_from_sig("tokenByIndex(uint256)")
        isMinter = Call.method_id_from_sig("isMinter(address)")

    pkid = DbPkidColumn(seq="addr_seq")
    addr_hx = Column(String(40), nullable=False, unique=True, primary_key=True)
    addr_hy = Column(String(34), nullable=False, unique=True, primary_key=True)
    addr_tp = Column(Enum(Type, validate_strings=True), nullable=False)
    block_h = Column(Integer, nullable=False, default=-1)
    info = DbInfoColumn()

    addr_users = relationship(
        "UserAddr",
        back_populates="addr",
        cascade="all, delete-orphan",
        single_parent=True,
    )

    addr_hist = relationship(
        "AddrHist",
        back_populates="addr",
        cascade="all, delete-orphan",
        single_parent=True,
    )

    def __str__(self):
        return self.addr_hy if self.addr_tp == Addr.Type.H else self.addr_hx

    def update_info(self, db: DB, block: Optional[Block]) -> bool:
        height = block.height if block is not None else db.rpc.getblockcount()

        if self.block_h == height:
            return False

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
            return False

        self.block_h = height

        for qrc721entry in info.get("qrc721Balances", []):
            qrc721entry["uris"] = self.nft_uris_from(db, qrc721entry["addressHex"], qrc721entry["count"])

        addr_hist: Optional[AddrHist] = None

        if block is not None:
            addr_hist = AddrHist(block=block, addr=self, info=dict(self.info))

        if self.info != info:
            self.info = info
            db.Session.add(self)

        if addr_hist is not None:
            if not len(self.addr_users):  # Should never happen, but...
                db.Session.delete(addr_hist)
                return False
            else:
                db.Session.add(addr_hist)

                for addr_user in self.addr_users:
                    addr_user.on_new_addr_hist(db, addr_hist)

                return True

        return False

    def nft_uris_from(self, db: DB, nft_addr_hx: str, count: int) -> dict:
        uris = {}

        for token_no in range(count):
            r = db.rpc.callcontract(
                nft_addr_hx,
                Addr.ContractMethodID.tokenOfOwnerByIndex
                + self.addr_hx.rjust(64, "0")
                + hex(token_no)[2:].rjust(64, "0")
            )

            if r.executionResult.excepted != "None":
                log.warning(f"Contract call tokenOfOwnerByIndex failed: {r.executionResult.excepted}")
                log.debug(f"Contract call failed (full result): {r}")
            else:
                token_id: str = hex(int(r.executionResult.output, 16))[2:]

                r = db.rpc.callcontract(
                    nft_addr_hx,
                    Addr.ContractMethodID.tokenURI
                    + r.executionResult.output
                )

                if r.executionResult.excepted != "None":
                    log.warning(f"Contract call tokenURI failed: {r.executionResult.excepted}")
                    log.debug(f"Contract call failed (full result): {r}")
                else:
                    token_uri = binascii.unhexlify(
                        r.executionResult.output
                    ).replace(b"\x00", b"").decode("utf-8")

                    uris[token_id] = token_uri

        return uris

    # noinspection PyUnusedLocal
    def __ensure_imported(self, db: DB):
        if self.addr_tp == Addr.Type.H:
            if self.addr_hy not in db.rpc.listlabels():
                log.info(f"Importing address {self.addr_hy}")
                db.rpc.importaddress(self.addr_hy, self.addr_hy)

    def __on_new_addr(self, db: DB):
        self.update_info(db, block=None)
        db.Session.commit()
        db.Session.refresh(self)

    def _removed_user(self, db: DB):
        if not len(self.addr_users):
            for addr_hist in list(self.addr_hist):
                addr_hist._remove(db, self.addr_hist)

            log.info(f"Deleting {self.addr_tp.value} address {str(self)} with no users.")
            db.Session.delete(self)
        else:
            for addr_hist in list(self.addr_hist):
                addr_hist._removed_user(db)

    @staticmethod
    def get(db: DB, address: str, create=True) -> Addr:
        addr_tp, addr_hx, addr_hy = Addr.normalize(db, address)

        try:
            q = db.Session.query(Addr).where(
                Addr.addr_hx == addr_hx
            )

            if not create:
                return q.one_or_none()

            return q.one()

        except NoResultFound:
            # noinspection PyArgumentList
            addr: Addr = Addr(addr_tp=addr_tp, addr_hx=addr_hx, addr_hy=addr_hy)
            addr.__on_new_addr(db)
            return addr

    @staticmethod
    @lru_cache(maxsize=None)
    def validate(db: DB, address: str):
        av = db.rpc.validateaddress(address)
        return av

    @staticmethod
    @lru_cache(maxsize=None)
    def normalize(db: DB, address: str) -> Tuple[Addr.Type, str, str]:
        """Normalize an input address into a tuple of (Addr.Type, addr_hex, addr_hydra).
        Or raise ValueError.
        """
        addr_tp = Addr.Type.by_len(address)

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

                addr_tp = Addr.__validate_contract(db, addr_hx)

            else:
                raise ValueError(f"Invalid HYDRA or smart contract address '{address}' (bad type)")

        except BaseRPC.Exception as exc:
            log.critical(f"Addr normalize RPC error: {str(exc)}", exc_info=exc)
            raise

        return addr_tp, addr_hx, addr_hy

    @staticmethod
    def __validate_contract(db: DB, addr_hx: str) -> Addr.Type:

        addr_tp = Addr.Type.S

        try:
            # Raises BaseRPC.Exception if address does not exist
            r = db.rpc.callcontract(addr_hx, Addr.ContractMethodID.name)
        except BaseRPC.Exception:
            # Safest assumption is that this is actually a HYDRA hex address
            return Addr.Type.H

        if r.executionResult.excepted != "None":
            return Addr.Type.H  # Dunno?

        r = db.rpc.callcontract(addr_hx, Addr.ContractMethodID.symbol)

        if r.executionResult.excepted == "None":
            # sci.symb = Addr.__sc_out_str(
            #     r.executionResult.output[128:]
            # )

            r = db.rpc.callcontract(addr_hx, Addr.ContractMethodID.totalSupply)

            if r.executionResult.excepted == "None":
                # sci.supt = int(r.executionResult.output, 16)

                r = db.rpc.callcontract(addr_hx, Addr.ContractMethodID.decimals)

                if r.executionResult.excepted == "None":
                    # sci.deci = int(r.executionResult.output, 16)
                    addr_tp = Addr.Type.T
                else:
                    # NFT contracts have no decimals()
                    addr_tp = Addr.Type.N

        return addr_tp

    @staticmethod
    def __sc_out_str(val):
        return binascii.unhexlify(val).replace(b"\x00", b"").decode("utf-8")
