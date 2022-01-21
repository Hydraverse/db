from __future__ import annotations

import asyncio
import time
from asyncio import CancelledError
from typing import Optional

from attrdict import AttrDict
from hydra import log
from sqlalchemy import Column, String, Integer, desc, UniqueConstraint, and_
from sqlalchemy.exc import NoResultFound
from sqlalchemy.orm import relationship

from .base import *
from .db import DB

__all__ = "Block",


class LocalState:
    # Testnet blocks:
    # 160387 (160388 is HYDRA SC TX + minting two tokens to sender)
    # 160544 (160545 is HYDRA TX)
    height = 0
    hash = ""


class Block(Base):
    __tablename__ = "block"
    __table_args__ = (
        UniqueConstraint("height", "hash"),
    )

    pkid = DbPkidColumn(seq="block_seq")
    height = Column(Integer, nullable=False, unique=False, primary_key=False, index=True)
    hash = Column(String(64), nullable=False, unique=True, primary_key=False, index=True)

    addr_hist = relationship(
        "AddrHist",
        back_populates="block",
        cascade="all, delete-orphan",
        single_parent=True,
    )

    info = DbInfoColumn()
    tx = DbDataColumn()

    def _removed_hist(self, db: DB):
        if not len(self.addr_hist):
            log.info(f"Deleting block #{self.height} with no history.")
            db.Session.delete(self)

    def on_new_block(self, db: DB) -> bool:
        info = Block.__get_block_info(db, self.hash)

        n_tx = info.get("nTx", -1)

        if n_tx < 2:
            log.warning(f"Found {n_tx} TX in block, expected at least two.")
            return False

        vo_filt = lambda vo: hasattr(vo, "scriptPubKey") and hasattr(vo.scriptPubKey, "addresses")

        addresses_hy = set()
        addresses_hx = set()
        self.tx = []

        block_logs = db.rpc.searchlogs(self.height, self.height)

        for txno, votx in enumerate(list(info.tx)):
            votx.n = txno  # Preserve ordering info after deletion.

            logs = list(filter(
                lambda lg_: lg_.transactionHash == votx.txid,
                block_logs
            ))

            vouts_inp = {}
            vouts_out = []

            if hasattr(votx, "vout"):
                vouts_inp = Block.__get_vout_inp(db.rpc, votx)
                vouts_out = [vout for vout in filter(vo_filt, votx.vout)]

            if len(vouts_out) or len(vouts_inp) or len(logs):
                tx = AttrDict(
                    block_txno=txno,
                    block_txid=votx.txid,
                    vouts_inp=vouts_inp,
                    vouts_out=vouts_out,
                    logs=logs
                )

                addrs_hy, addrs_hx = Block.addrs_from_tx(tx)
                addresses_hy.update(addrs_hy)
                addresses_hx.update(addrs_hx)

                self.tx.append(tx)
                info.tx.remove(votx)

        self.info = info

        if not len(addresses_hy) and not len(addresses_hx):
            return False

        from .addr import Addr

        # noinspection PyUnresolvedReferences
        addrs: List[Addr] = db.Session.query(
            Addr,
        ).where(
            and_(
                # Addr.addr_tp == Addr.Type.H,
                or_(
                    Addr.addr_hy.in_(addresses_hy),
                    Addr.addr_hx.in_(addresses_hx),
                )
            )
        ).all()

        if not len(addrs):
            return False

        added_history = False

        for addr in addrs:
            added_history |= addr.update_info(db, block=self)

        return added_history

    @staticmethod
    def addrs_from_tx(tx: AttrDict):
        addresses_hy = set()
        addresses_hx = set()

        vo_filt = lambda vo: "scriptPubKey" in vo and "addresses" in vo["scriptPubKey"]

        for vout in filter(vo_filt, tx.vouts_out):
            addresses_hy.update(vout["scriptPubKey"]["addresses"])

        for vout in filter(vo_filt, tx.vouts_inp.values()):
            addresses_hy.update(vout["scriptPubKey"]["addresses"])

        for log_ in tx.logs:
            if "contractAddress" in log_:
                addresses_hx.add(log_["contractAddress"])

            if "from" in log_:
                addresses_hx.add(log_["from"])

            if "to" in log_:
                addresses_hx.add(log_["to"])

            for log__ in log_.log:
                addresses_hx.add(log__["address"])

        return addresses_hy, addresses_hx

    @staticmethod
    def get(db: DB, height: int, create: Optional[bool] = True) -> Optional[Block]:
        try:
            block = db.Session.query(
                Block,
            ).where(
                Block.height == height,
            )

            if not create:
                return block.one_or_none()

            return block.one()
        except NoResultFound:
            return Block.make(db, height)

    @staticmethod
    def make(db: DB, height: int) -> Optional[Block]:
        bhash = db.rpc.getblockhash(height)

        # noinspection PyArgumentList
        new_block: Block = Block(
            height=height,
            hash=bhash,
        )

        db.Session.add(new_block)

        if new_block.on_new_block(db):
            db.Session.commit()
            db.Session.refresh(new_block)
            log.info(f"Added block with {len(new_block.addr_hist)} history entries at height {new_block.height}")
            return new_block

        log.debug(f"Discarding block without TXes at height {new_block.height}")
        db.Session.rollback()

    @staticmethod
    async def update_task_async(db: DB) -> None:
        # await asyncio.sleep(30)

        try:
            while 1:
                await Block.update_async(db)
                await asyncio.sleep(15)
        except KeyboardInterrupt:
            pass
        except CancelledError:
            pass

    @staticmethod
    async def update_async(db: DB) -> None:
        # noinspection PyBroadException
        try:
            if LocalState.height == 0:
                await db.in_session_async(Block.__update_init, db)

            return await db.in_session_async(Block.update, db)
        except KeyboardInterrupt:
            raise
        except CancelledError:
            raise
        except BaseException as exc:
            log.critical(f"Block.update exception: {str(exc)}", exc_info=exc)

    @staticmethod
    def update_task(db: DB) -> None:
        try:
            if LocalState.height == 0:
                Block.__update_init(db)

            while 1:
                Block.update(db)
                time.sleep(15)

        except KeyboardInterrupt:
            pass

    @staticmethod
    def __update_init(db: DB) -> None:
        block: Block = db.Session.query(
            Block
        ).order_by(
            desc(Block.height)
        ).limit(1).one_or_none()

        if block is not None:
            LocalState.height = block.height
            LocalState.hash = block.hash
        else:
            LocalState.height = db.rpc.getblockcount() - 1

    @staticmethod
    def update(db: DB) -> None:

        chain_height = db.rpc.getblockcount()
        chain_hash = db.rpc.getblockhash(chain_height)

        log.debug(f"Poll: chain={chain_height} local={LocalState.height}")

        if chain_height == LocalState.height:
            if chain_hash != LocalState.hash:
                log.warning(f"Fork detected at height {chain_height}: {chain_hash} != {LocalState.hash}")
            else:
                return

        for height in range(LocalState.height + 1, chain_height + 1):
            Block.make(db, height)

        LocalState.height = chain_height
        LocalState.hash = chain_hash

    @staticmethod
    def __get_block_info(db: DB, block_hash: str):
        info = db.rpc.getblock(block_hash, verbosity=2)

        info.conf = info.confirmations
        del info.confirmations
        del info.hash
        del info.height

        return info

    @staticmethod
    def __get_vout_inp(rpc, tx) -> dict:
        vout_inp = {}

        if hasattr(tx, "vin"):
            for vin in filter(lambda vin_: hasattr(vin_, "txid"), tx.vin):

                vin_rawtx = rpc.getrawtransaction(vin.txid, False)

                vin_rawtx_decoded = rpc.decoderawtransaction(vin_rawtx, True)

                vout_inp[vin.txid] = vin_rawtx_decoded.vout[vin.vout]

        return vout_inp

