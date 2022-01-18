from __future__ import annotations

from typing import List

from hydra import log
from sqlalchemy import Column, ForeignKey, Integer, UniqueConstraint, Index, or_, and_
from sqlalchemy.orm import relationship

from .base import *
from .db import DB
from .tx import TX
from .user_addr_tx import UserAddrTX

__all__ = "AddrTX",


class AddrTX(DbPkidMixin, Base):
    __tablename__ = "addr_tx"
    __table_args__ = (
        UniqueConstraint("addr_pk", "tx_pk"),
        Index(__tablename__ + "_idx", "addr_pk", "tx_pk")
    )

    addr_pk = Column(Integer, ForeignKey("addr.pkid", ondelete="CASCADE"), nullable=False, primary_key=False)
    tx_pk = Column(Integer, ForeignKey("tx.pkid", ondelete="CASCADE"), nullable=False, primary_key=False)

    addr = relationship("Addr", back_populates="addr_txes")
    tx = relationship("TX", back_populates="addr_txes")

    addr_tx_users = relationship(
        UserAddrTX,
        back_populates="addr_tx",
        cascade="all, delete-orphan",
        single_parent=True,
    )

    def _removed_user(self, db: DB):
        if not len(self.addr_tx_users):
            log.info(f"Deleting Block {self.tx.block.height} TX {self.tx.block_txno} for addr {str(self.addr)} with no users.")
            self._remove(db, self.addr.addr_txes)

    def _remove(self, db: DB, addr_txes):
        tx = self.tx
        addr_txes.remove(self)
        tx._removed_addr(db)

    def on_new_addr_tx(self, db: DB) -> bool:
        tx = self.tx

        if not self.addr.on_new_addr_tx(db, self):
            tx.addr_txes.remove(self)
            tx._removed_addr(db)
            return False

        return True

    @staticmethod
    def on_new_block_tx(db: DB, tx: TX) -> bool:
        """Correspond Addr's to TX."""

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

        added = 0
        removed = 0

        addr_txes = []

        for addr in addrs:
            # noinspection PyArgumentList
            addr_tx = AddrTX(addr=addr, tx=tx)
            db.Session.add(addr_tx)
            added += 1
            addr_txes.append(addr_tx)

        if added > 0:
            # db.Session.commit()

            # addr_txes: List[AddrTX] = db.Session.query(
            #     AddrTX,
            # ).where(AddrTX.tx_pk == tx.pkid).all()

            # Call after list is fully formed
            for addr_tx in addr_txes:
                if not addr_tx.on_new_addr_tx(db):
                    removed += 1

        if removed > 0:
            # db.Session.commit()
            pass

        added -= removed

        return added > 0
