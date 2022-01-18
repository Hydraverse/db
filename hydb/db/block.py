from __future__ import annotations

import asyncio
from asyncio import CancelledError
from typing import Optional

from attrdict import AttrDict
from hydra import log
from sqlalchemy import Column, String, Integer, desc, UniqueConstraint, and_
from sqlalchemy.exc import NoResultFound
from sqlalchemy.orm import relationship

from .base import *
from .db import DB

__all__ = "Block", "TX"


class LocalState:
    # Testnet blocks:
    # 160387 (160388 is HYDRA SC TX + minting two tokens to sender)
    # 160544 (160545 is HYDRA TX)
    height = 0
    hash = ""


class Block(DbPkidMixin, Base):
    __tablename__ = "block"
    __table_args__ = (
        UniqueConstraint("height", "hash"),
        DbInfoColumnIndex(__tablename__, "info"),
        DbInfoColumnIndex(__tablename__, "logs"),
    )

    height = Column(Integer, nullable=False, unique=False, primary_key=False, index=True)
    hash = Column(String(64), nullable=False, unique=True, primary_key=False, index=True)

    txes = relationship(
        "TX",
        back_populates="block",
        cascade="all, delete-orphan",
        single_parent=True
    )

    info = DbInfoColumn()
    logs = DbInfoColumn()

    def _delete_if_unused(self, db: DB) -> bool:
        if not len(self.txes):
            # if not len(self.user_data):
            log.info(f"Deleting block #{self.height} with no TXes.")
            db.Session.delete(self)
            return True
            # else:
            #     log.info(f"Keeping block #{self.height} with no TXes and non-empty data.")

        return False

    def on_new_block(self, db: DB) -> bool:
        n_tx = self.info.get("nTx", -1)

        if n_tx < 2:
            log.warning(f"Found {n_tx} TX in block, expected at least two.")
            return False

        vo_filt = lambda vo: hasattr(vo, "scriptPubKey") and hasattr(vo.scriptPubKey, "addresses")

        txes = []
        add = 0
        rem = 0

        for txno, votx in enumerate(list(self.info["tx"])):
            votx.n = txno  # Preserve ordering info after deletion.

            logs = list(
                self.__remove_log(lg)
                for lg in filter(
                    lambda lg_: lg_.transactionHash == votx.txid,
                    tuple(self.logs)
                )
            )

            if hasattr(votx, "vout"):
                vouts_inp = Block.__get_vout_inp(db.rpc, votx)
                vouts_out = [vout for vout in filter(vo_filt, votx.vout)]

                if len(vouts_out):
                    # noinspection PyArgumentList
                    tx = TX(
                        block=self,
                        block_txno=txno,
                        block_txid=votx.txid,
                        vouts_inp=vouts_inp,
                        vouts_out=vouts_out,
                        logs=logs
                    )

                    txes.append((votx, tx))

                    add += 1

        if add > 0:
            for votx, tx in txes:
                if not tx.on_new_block(db):
                    rem += 1
                else:
                    self.info["tx"].remove(votx)  # Leave behind the unprocessed TXes

            add -= rem

        return add > 0

    def __remove_log(self, lo):
        _lg = AttrDict(lo)
        self.logs.remove(lo)
        del _lg.blockHash
        del _lg.blockNumber
        del _lg.transactionHash
        del _lg.transactionIndex
        return _lg

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
            info=Block.__get_block_info(db, bhash),
            logs=db.rpc.searchlogs(height, height)
        )

        db.Session.add(new_block)

        if new_block.on_new_block(db):
            db.Session.commit()
            db.Session.refresh(new_block)
            log.info(f"Added block with {len(new_block.txes)} TX(es) at height {new_block.height}")
            return new_block

        log.debug(f"Discarding block without TXes at height {new_block.height}")
        db.Session.rollback()

    @staticmethod
    def _on_new_addr(db: DB, addr) -> Optional[Block]:
        """Load the latest block & related txes for addr.
        """
        #
        # TODO: Implement this if preloading addresses.
        # if addr.addr_tp == addr.Type.H:
        #     txes = db.rpc.listtransactions(label=addr.addr_hy, count=1, skip=0, include_watchonly=True)
        #
        #     if not len(txes):
        #         return
        #
        pass

    @staticmethod
    async def update_task(db: DB) -> None:
        # await asyncio.sleep(30)

        try:
            while 1:
                await Block.update(db)
                await asyncio.sleep(15)
        except KeyboardInterrupt:
            pass
        except CancelledError:
            pass

    @staticmethod
    async def update(db: DB) -> None:
        # noinspection PyBroadException
        try:
            if LocalState.height == 0:
                await db.run_in_executor_session(Block.__update_init, db)

            return await db.run_in_executor_session(Block.__update, db)
        except KeyboardInterrupt:
            raise
        except CancelledError:
            raise
        except BaseException as exc:
            log.critical(f"Block.update exception: {str(exc)}", exc_info=exc)

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
    def __update(db: DB) -> None:

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


from .tx import TX
