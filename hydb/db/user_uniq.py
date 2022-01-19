from time import time_ns
from cryptography.fernet import Fernet

from sqlalchemy import Column, ForeignKey, Integer, String, BigInteger, LargeBinary
from sqlalchemy.orm import relationship, declared_attr

from .base import *
from .db import DB
from hydb.util import namegen

__all__ = "UserUniq",


@dictattrs("pkid", "date_create", "date_update", "name", "time", "nano", "info", "data")
class UserUniq(Base):
    __tablename__ = "user_uniq"
    __table_args__ = (
        DbInfoColumnIndex(__tablename__),
    )

    pkid = DbPkidColumn(seq="user_uniq_seq")
    date_create = DbDateCreateColumn()
    date_update = DbDateUpdateColumn()
    name = Column(String, nullable=False, unique=True)
    time = Column(BigInteger, nullable=False, unique=True)
    nano = Column(BigInteger, nullable=False, unique=False)
    fkey = Column(LargeBinary(140), nullable=False, unique=True)

    addr_shr_hy = Column(String(34), nullable=True, unique=True, index=False)
    addr_shr_pk = Column(LargeBinary(164), nullable=True, unique=False, index=False)
    addr_loc_hy = Column(String(34), nullable=True, unique=True, index=False)
    addr_loc_pk = Column(LargeBinary(164), nullable=True, unique=False, index=False)

    info = DbInfoColumn()
    data = DbDataColumn()

    # noinspection PyUnusedLocal
    def __init__(self, db: DB):
        ts_ns = td_ns = time_ns()
        name = " ".join(namegen.make_name())
        fkey = db.fernet.encrypt(Fernet.generate_key())
        td_ns = time_ns() - td_ns
        super().__init__(name=name, time=ts_ns, nano=td_ns, fkey=fkey)

    @staticmethod
    def make_name():
        return " ".join(namegen.make_name())


class DbUserUniqMixin:

    @declared_attr
    def pkid(self):
        return Column(Integer, ForeignKey("user_uniq.pkid", ondelete="CASCADE"), nullable=False, unique=True, primary_key=True, index=True)

    # name = Column(String, ForeignKey("user_uniq.name"), nullable=False, unique=True, primary_key=True, index=True)

    @declared_attr
    def uniq(self):
        return relationship("UserUniq")
