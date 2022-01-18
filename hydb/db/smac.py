from typing import Optional

from sqlalchemy import Column, Integer, ForeignKey, String

from hydra import log
from hydra.rpc.base import BaseRPC
from hydra.app.call import Call

from .base import DbDataColumn, dictattrs
from .db import DB
from .addr import Addr
from .block import Block, TX

__all__ = "Smac", "Tokn", "NFT"


@dictattrs("name")
class Smac(Addr):
    __tablename__ = "smac"
    __mapper_args__ = {
        "polymorphic_identity": Addr.Type.S,
    }

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

    pkid = Column(Integer, ForeignKey("addr.pkid"), nullable=False, primary_key=True)
    name = Column(String, nullable=False, index=True)
    stor = DbDataColumn()

    def __str__(self):
        return self.addr_hx

    def on_new_block(self, db: DB, block: Block):
        # TODO: Optionally (?) also update name & Addr.validate_contract() info.
        super().on_new_block(db, block)

        try:
            stor = db.rpc.getstorage(self.addr_hx, self.block_h)
        except BaseRPC.Exception as exc:
            log.critical(f"Smac RPC error: {str(exc)}", exc_info=exc)
            pass
        else:
            if stor != self.stor:
                self.stor = stor
                db.Session.add(self)

    def update_balances(self, db, tx: Optional[TX]):
        # Contract HYDRA balance retrieved by Addr.
        return super().update_balances(db, tx)


from .tokn import Tokn
from .nft import NFT
