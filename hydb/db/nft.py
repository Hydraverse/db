import binascii
from typing import Optional

from sqlalchemy import Column, Integer, ForeignKey

from hydra import log
from hydra.rpc.base import BaseRPC

from .db import DB
from .addr import Addr
from .smac import Smac
from .tokn import Tokn

__all__ = "NFT",


class NFT(Tokn):
    __tablename__ = "nft"
    __mapper_args__ = {
        "polymorphic_identity": Addr.Type.N,
    }

    pkid = Column(Integer, ForeignKey("tokn.pkid"), nullable=False, primary_key=True)

    def asdict(self):
        d = super().asdict()

        if "deci" in d:
            del d.deci

        return d

    def nft_tokn_uris_for(self, db: DB, addr: Addr, balance: Optional[int] = None) -> Optional[dict]:
        if balance is None:
            balance = self.balance_of(db, addr)

        if balance:
            try:
                return self.__nft_tokn_uris_for(db, addr, balance)
            except BaseRPC.Exception as exc:
                log.critical(f"NFT RPC error: {str(exc)}", exc_info=exc)

    def __nft_tokn_uris_for(self, db: DB, addr: Addr, count: int) -> dict:
        tokens = {}

        for token_no in range(count):
            r = db.rpc.callcontract(
                self.addr_hx,
                Smac.ContractMethodID.tokenOfOwnerByIndex
                + addr.addr_hx.rjust(64, "0")
                + hex(token_no)[2:].rjust(64, "0")
            )

            if r.executionResult.excepted != "None":
                log.warning(f"Contract call tokenOfOwnerByIndex failed: {r.executionResult.excepted}")
                log.debug(f"Contract call failed (full result): {r}")
            else:
                token_id = int(r.executionResult.output, 16)

                r = db.rpc.callcontract(
                    self.addr_hx,
                    Smac.ContractMethodID.tokenURI
                    + hex(token_id)[2:].rjust(64, "0")
                )

                if r.executionResult.excepted != "None":
                    log.warning(f"Contract call tokenURI failed: {r.executionResult.excepted}")
                    log.debug(f"Contract call failed (full result): {r}")
                else:
                    token_uri = binascii.unhexlify(
                        r.executionResult.output
                    ).replace(b"\x00", b"").decode("utf-8")

                    tokens[token_id] = token_uri

        return tokens
