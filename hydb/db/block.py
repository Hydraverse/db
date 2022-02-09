from __future__ import annotations

import time
from typing import Optional, List

import sqlalchemy.orm.exc
from hydra import log
from hydra.rpc import BaseRPC
from requests import RequestException
from sqlalchemy import Column, String, Integer, desc, UniqueConstraint, and_, or_, asc, func
from sqlalchemy.exc import NoResultFound
from sqlalchemy.orm import relationship

from .base import *
from .db import DB
from ..api import schemas

__all__ = "Block",


class LocalState:
    # Testnet blocks:
    # 160387 (160388 is HYDRA SC TX + minting two tokens to sender)
    # 160544 (160545 is HYDRA TX)
    # 175377 (175378 is NFT TX)
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
    conf = Column(Integer, nullable=False, index=True)
    info = DbInfoColumn()
    tx = DbDataColumn()

    addr_hist = relationship(
        "AddrHist",
        order_by="AddrHist.pkid",
        back_populates="block",
        cascade="all, delete-orphan",
        single_parent=True,
    )

    CONF_MATURE = 501

    def _removed_hist(self, db: DB):
        if not len(self.addr_hist):
            log.info(f"Deleting block #{self.height} with no history.")
            db.Session.delete(self)

    def on_new_block(self, db: DB, chain_height: int) -> bool:
        info, txes = None, None

        while 1:
            # noinspection PyBroadException
            try:
                info, txes = self.__get_block_info(db)
                break
            except BaseRPC.Exception as exc:
                if exc.response.status_code == 404:
                    # Block not in explorer yet, so wait for a little while.
                    log.warning(f"Block #{self.height} not in explorer yet, trying again in 10s.")
                    time.sleep(10)
                    continue

                log.error(f"RPC error querying explorer API: {str(exc)}. (Retrying in 30s)", exc_info=exc)
                time.sleep(30)
                continue
            except BaseException as exc:
                log.error(f"Error querying explorer API: {str(exc)}. (Retrying in 60s)", exc_info=exc)
                time.sleep(60)
                continue

        addresses_hy = set()
        addresses_hx = set()

        for tx in txes:
            for address in schemas.Block.tx_yield_addrs(tx):
                if len(address) == 34:
                    addresses_hy.add(address)
                elif len(address) == 40:
                    addresses_hx.add(address)
                else:
                    log.warning(f"Unknown address length {len(address)}: '{address}'")

        if not len(addresses_hy) and not len(addresses_hx):
            return False

        from .addr import Addr

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

        self.conf = info["confirmations"]
        del info["confirmations"]
        self.info = info
        self.tx = txes

        added_history = False

        for addr in addrs:
            added_history |= addr.on_block_create(db=db, block=self)

        if self.height == chain_height:
            self.make_stat(db)

        return added_history

    def make_stat(self, db: DB):
        from .stat import Stat

        try:
            if not Stat.exists_for_block(db, self):
                stat = Stat(db=db, block=self)

                db.Session.add(stat)
                db.Session.commit()
                db.Session.refresh(stat)

                log.info(f"Stat #{stat.pkid} Block #{self.height}")

        except sqlalchemy.exc.SQLAlchemyError as exc:
            log.warning(f"Exception while adding new Stat entry: {exc}", exc_info=exc)
        except BaseRPC.Exception as exc:
            log.warning(f"RPC Exception while adding new Stat entry: {exc}", exc_info=exc)
        except BaseException as exc:
            log.warning(f"Other Exception while adding new Stat entry: {exc}", exc_info=exc)

    def filter_tx(self, address: str):
        return filter(lambda tx: address in list(schemas.Block.tx_yield_addrs(tx)), self.tx)

    def on_fork(self, db: DB, hash_new: str):
        if self.hash == hash_new:
            return

        log.critical(f"Fork detected at height {self.height}: {self.hash} != {hash_new}")
        log.error(f"Reverting {len(self.addr_hist)} addresses.")

        for addr_hist in self.addr_hist:
            addr_hist.on_fork(db)

        height = self.height

        db.Session.delete(self)
        # Deletes all related addr_hist and user_addr_hist.
        db.Session.commit()

        log.error(f"Re-processing block #{self.height}")

        Block.make(
            db=db,
            height=height,
            chain_height=-1,
            block_hash=hash_new
        )

    def update_confirmations(self, db: DB):
        block_hash = db.rpc.getblockhash(self.height)

        if self.hash != block_hash:
            self.on_fork(db, block_hash)
            return

        try:
            block_header = db.rpc.getblockheader(blockhash=self.hash)
        except BaseRPC.Exception as exc:
            log.warning(f"Block call to getblockheader() failed: {exc}", exc_info=exc)
            return

        conf = block_header.confirmations

        if conf >= Block.CONF_MATURE:

            while 1:
                try:
                    self.conf = conf

                    for addr_hist in self.addr_hist:
                        addr_hist.on_update_conf(db)

                    db.Session.add(self)
                    db.Session.commit()

                    break

                except sqlalchemy.exc.SQLAlchemyError as exc:
                    log.error(
                        f"Block.update_confirmations(): Got SQL error '{exc}', trying again.", exc_info=exc
                    )

                    db.Session.rollback()

                    try:
                        db.Session.refresh(self)
                        continue
                    except sqlalchemy.exc.SQLAlchemyError as exc:
                        log.error(
                            f"Block.update_confirmations(refresh): Got SQL error '{exc}', not trying again.", exc_info=exc
                        )

                        return False

            try:
                if self.update_confirmations_post_commit(db):
                    db.Session.commit()

            except sqlalchemy.exc.SQLAlchemyError as exc:
                log.error(
                    f"Block.update_confirmations(post-commit): Got SQL error '{exc}', not trying again.", exc_info=exc
                )

    def update_confirmations_post_commit(self, db: DB) -> bool:
        """Called after bulk commit when update_confirmations() returned True.
        """
        if self.conf < Block.CONF_MATURE:
            return False

        if self.conf > Block.CONF_MATURE or not len(self.addr_hist):
            log.debug(f"Delete over-mature block #{self.height} with {self.conf} confirmations and {len(self.addr_hist)} hist entries.")
            db.Session.delete(self)
            return True

        if self.conf == Block.CONF_MATURE:  # Decision: Only process when timing is right, to enable self deletion.

            if len(self.addr_hist):
                for addr_hist in self.addr_hist:
                    addr_hist.on_block_mature(db)

                try:
                    db.api.sse_block_notify_mature(block_pk=self.pkid)
                except BaseRPC.Exception as exc:
                    log.error(f"Unable to send block mature notify: response={exc.response} error={exc.error}", exc_info=exc)
                except RequestException as exc:
                    log.error(f"Unable to send block mature notify: request={exc.request} response={exc.response}", exc_info=exc)
                except BaseException as exc:
                    log.error(f"Unable to send block mature notify: {exc}", exc_info=exc)
                else:
                    log.debug(f"Sent notification for matured block #{self.pkid}")

        return False

    @staticmethod
    def update_confirmations_all(db: DB):
        """Update confirmations on stored blocks.
        """
        while 1:
            blocks: List[Block] = db.Session.query(
                Block
            ).order_by(
                asc(Block.height)
            ).all()

            try:
                for block in blocks:
                    block.update_confirmations(db)

                break

            except sqlalchemy.exc.SQLAlchemyError as exc:
                log.error(
                    f"Block.update_confirmations_all(): Got SQL error '{exc}', trying again.", exc_info=exc
                )

                db.Session.rollback()
                continue

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
    def make(db: DB, height: int, chain_height: int, block_hash: Optional[str] = None) -> Optional[Block]:
        while 1:
            try:
                bhash = db.rpc.getblockhash(height) if block_hash is None else block_hash

                # noinspection PyArgumentList
                new_block: Block = Block(
                    height=height,
                    hash=bhash,
                )

                if new_block.on_new_block(db, chain_height):
                    db.Session.add(new_block)
                    db.Session.commit()
                    db.Session.refresh(new_block)
                    log.info(f"Processed block #{new_block.height}  chain: {chain_height}  hist: {len(new_block.addr_hist)}")

                    try:
                        db.api.sse_block_notify_create(block_pk=new_block.pkid)
                    except BaseRPC.Exception as exc:
                        log.error(f"Unable to send block notify: response={exc.response} error={exc.error}", exc_info=exc)
                    except RequestException as exc:
                        log.error(f"Unable to send block notify: request={exc.request} response={exc.response}", exc_info=exc)
                    except BaseException as exc:
                        log.error(f"Unable to send block notify: {exc}", exc_info=exc)
                    else:
                        log.debug(f"Sent notification for new block #{new_block.pkid}")

                    return new_block

            except sqlalchemy.orm.exc.StaleDataError as exc:
                log.error("Block.make(): Got StaleDataError when attempting to create new block, trying again.", exc_info=exc)
                db.Session.rollback()
                continue
            except sqlalchemy.exc.SQLAlchemyError as exc:
                log.error(f"Block.make(): Got SQL error {exc}, trying again.", exc_info=exc)
                db.Session.rollback()
                continue
            except BaseRPC.Exception as exc:
                log.error("Block.make(): RPC error, trying again.", exc_info=exc)
                db.Session.rollback()
                continue

            log.debug(f"Discarding block without history entries at height {new_block.height}")
            db.Session.rollback()
            break

    @staticmethod
    def update_task(db: DB) -> None:
        try:
            Block.__update_init(db)

            Block.update_confirmations_all(db)

            while 1:
                if Block.update(db):
                    Block.update_confirmations_all(db)
                time.sleep(1)

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
            if LocalState.height == 0:
                LocalState.height = db.rpc.getblockcount() - 1

    @staticmethod
    def update(db: DB) -> bool:

        chain_height = db.rpc.getblockcount()
        chain_hash = db.rpc.getblockhash(chain_height)

        # log.debug(f"Poll: chain={chain_height} local={LocalState.height}")

        if chain_height == LocalState.height:
            if LocalState.hash and chain_hash != LocalState.hash:
                log.warning(f"Fork detected at height {chain_height}: {chain_hash} != {LocalState.hash}")
            else:
                return False

        for height in range(LocalState.height + 1, chain_height + 1):
            Block.make(db, height, chain_height)

        LocalState.height = chain_height
        LocalState.hash = chain_hash

        return True

    def __get_block_info(self, db: DB):
        while 1:
            if self.hash is None:
                self.hash = db.rpc.getblockhash(self.height)

            info = db.rpcx.get_block(self.height)

            if isinstance(info, str):
                log.warning("get_block_info(): Explorer seems under maintenance, trying again in 30s.")
                time.sleep(30)
                continue

            if info.height != self.height or info.hash != self.hash:
                log.warning(f"Block info mismatch at height {self.height}/{self.hash} != {info.height}/{info.hash} -- retrying in 60s.")
                self.hash = None
                time.sleep(60)
                continue

            del info.hash
            del info.height

            tx = []

            for txid in info.transactions:
                trxn = db.rpcx.get_tx(txid)

                if isinstance(trxn, str):
                    log.warning(f"get_block_info({self.height}:{txid}): Transaction could not load, skipping.")
                    continue

                tx.append(trxn)

            return info, tx

