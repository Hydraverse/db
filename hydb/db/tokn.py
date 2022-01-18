from typing import Optional

from sqlalchemy import Column, Integer, ForeignKey, String, BigInteger
from sqlalchemy.orm import relationship

from hydra import log
from hydra.rpc.base import BaseRPC

from .base import dictattrs
from .db import DB
from .addr import Addr
from .smac import Smac
from .tx import TX

__all__ = "Tokn", "ToknAddr"


@dictattrs("symb", "deci", "supt")
class Tokn(Smac):
    __tablename__ = "tokn"
    __mapper_args__ = {
        "polymorphic_identity": Addr.Type.T,
    }

    pkid = Column(Integer, ForeignKey("smac.pkid"), nullable=False, primary_key=True)
    symb = Column(String, nullable=False)
    supt = Column(BigInteger, nullable=False)
    deci = Column(Integer, nullable=True)

    tokn_addrs = relationship(
        "ToknAddr",
        back_populates="tokn",
        cascade="all, delete-orphan",
        single_parent=True,
    )

    def __str__(self):
        return self.addr_hx

    def balance_of(self, db: DB, addr: Addr) -> Optional[int]:
        try:
            # NOTE: Currently removed by Addr.update_balances()
            # for qbal in addr.info.get("qrc20Balances", {}):
            #     if qbal.get("addressHex", ...) == self.addr_hx:
            #         return qbal["balance"]

            return self.__balance_of_rpc(db, addr)
        except BaseRPC.Exception as exc:
            log.critical(f"Tokn RPC error: {str(exc)}", exc_info=exc)
            return None

    def __balance_of_rpc(self, db: DB, addr: Addr) -> Optional[int]:
        r = db.rpc.callcontract(
            self.addr_hx,
            Smac.ContractMethodID.balanceOf + addr.addr_hx.rjust(64, "0")
        )

        if r.executionResult.excepted != "None":
            log.warning(f"Contract call failed: {r.executionResult.excepted}")
            log.debug(f"Contract call failed (full result): {r}")
            return None

        return int(r.executionResult.output, 16)

    def apply_deci(self, balance: int) -> str:
        if self.deci is None:
            return str(balance)

        return str(balance / 10**self.deci)  # TODO: Apply decimal manually or use library.

    def update_balances(self, db: DB, tx: Optional[TX]):
        super().update_balances(db, tx)

        if tx is not None:
            for tx_addr in filter(lambda txa: txa.addr, tx.addr_txes):
                tokn_addr = ToknAddr.get_for(db, self, tx_addr.addr, create=True)
                tokn_addr.update_balance(db, tx)


from .tokn_addr import ToknAddr
