from time import time_ns
from typing import List

from cryptography.fernet import Fernet
from hydra import log

from sqlalchemy import Column, ForeignKey, Integer, String, BigInteger, LargeBinary
from sqlalchemy.orm import relationship

from .base import *
from .db import DB
from hydb.util import namegen

__all__ = "UserUniq", "DbUserUniqPkidColumn", "DbUserUniqRelationship"


class UserUniq(Base):
    __tablename__ = "user_uniq"
    __table_args__ = (
        DbInfoColumnIndex(__tablename__),
    )

    pkid = DbPkidColumn(seq="user_uniq_seq")
    date_create = DbDateCreateColumn()
    time_create = Column(BigInteger, nullable=False, unique=True)
    name_weight = Column(BigInteger, nullable=False, unique=False)
    name = Column(String, nullable=False, unique=True)
    fkey = Column(LargeBinary(140), nullable=False, unique=True)

    hyve_addr_hy = Column(String(34), nullable=False, unique=True, index=False)
    hyve_addr_pk = Column(LargeBinary(164), nullable=False, unique=False, index=False)
    base_addr_hy = Column(String(34), nullable=False, unique=True, index=False)
    base_addr_pk = Column(LargeBinary(164), nullable=False, unique=False, index=False)

    info = DbInfoColumn()
    data = DbDataColumn()

    # noinspection PyUnusedLocal
    def __init__(self, db: DB):
        names = [row.name for row in db.session.query(UserUniq.name).all()]

        while 1:
            ts_ns = td_ns = time_ns()
            name = " ".join(namegen.make_name())
            td_ns = time_ns() - td_ns

            if name not in names:
                break

            log.error(f"UserUniq name clash! '{name}'")

        fkey = Fernet.generate_key()
        uniq_fernet = Fernet(fkey)

        hyve_addr_hy = db.rpc.getnewaddress(label=name)
        hyve_addr_pk = uniq_fernet.encrypt(bytes(db.rpc.dumpprivkey(address=hyve_addr_hy), encoding="utf-8"))
        base_addr_hy = db.rpc.getnewaddress(label=name)
        base_addr_pk = uniq_fernet.encrypt(bytes(db.rpc.dumpprivkey(address=base_addr_hy), encoding="utf-8"))

        fkey = db.fernet.encrypt(fkey)

        super().__init__(
            time_create=ts_ns,
            name_weight=td_ns,
            name=name,
            fkey=fkey,
            hyve_addr_hy=hyve_addr_hy,
            hyve_addr_pk=hyve_addr_pk,
            base_addr_hy=base_addr_hy,
            base_addr_pk=base_addr_pk,
        )

    @staticmethod
    def make_name():
        return " ".join(namegen.make_name())

    def fernet(self, db: DB) -> Fernet:
        return Fernet(db.fernet.decrypt(self.fkey))

    def decrypt_hyve_addr_pk(self, db: DB):
        return str(self.fernet(db).decrypt(self.hyve_addr_pk), encoding="utf-8")

    def decrypt_base_addr_pk(self, db: DB):
        return str(self.fernet(db).decrypt(self.base_addr_pk), encoding="utf-8")

    @staticmethod
    def check_wallet_addrs(db: DB):
        with db.with_session():
            users: List[UserUniq] = db.session.query(
                UserUniq
            ).all()

        labels = db.rpc.listlabels()

        for user in users:
            addrs = UserUniq.label_addrs(db, labels, user.name)

            if user.hyve_addr_hy not in addrs:
                log.warning(f"Importing {user.name} hyve private key.")
                db.rpc.importprivkey(
                    hydraprivkey=user.decrypt_hyve_addr_pk(db),
                    label=user.name,
                    rescan=True
                )
                log.info(f"Imported {user.name} hyve private key.")

            if user.base_addr_hy not in addrs:
                log.warning(f"Importing {user.name} base private key.")
                db.rpc.importprivkey(
                    hydraprivkey=user.decrypt_base_addr_pk(db),
                    label=user.name,
                    rescan=True
                )
                log.info(f"Imported {user.name} base private key.")

    @staticmethod
    def label_addrs(db: DB, labels: List[str], label: str):
        return db.rpc.getaddressesbylabel(label).keys() if label in labels else []


DbUserUniqPkidColumn = lambda: Column(Integer, ForeignKey("user_uniq.pkid", ondelete="CASCADE"), nullable=False, unique=True, primary_key=True, index=True)
DbUserUniqRelationship = lambda: relationship("UserUniq")
