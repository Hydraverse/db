from attrdict import AttrDict
from sqlalchemy import Column, DateTime, func, Integer, Sequence, Index
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import declarative_base
from sqlalchemy_json import mutable_json_type

__all__ = (
    "Base", "dictattrs",
    "DbPkidMixin", "DbDateMixin",
    "DbInfoColumn", "DbDataColumn",
    "DbInfoColumnIndex",
)


def dictattrs(*attrs):
    def _asdict(self, attrs_) -> AttrDict:
        def _attr_conv(s, attr):
            attr = getattr(s, attr)
            if hasattr(attr, "_asdict"):
                return attr._asdict()
            if isinstance(attr, (list, tuple)):
                return [_attr_conv(attr, a) for a in attr]
            return attr

        return AttrDict({
            attr: _attr_conv(self, attr)
            for attr in attrs_
        })

    def _cls(cls):
        cls.__dictattrs__ = getattr(cls, "__dictattrs__", ()) + attrs
        cls._asdict = lambda slf, *atrs_: _asdict(slf, tuple(cls.__dictattrs__) + atrs_)

        return cls

    return _cls


class Base:
    __mapper_args__ = {"eager_defaults": True}

    def asdict(self) -> AttrDict:
        return getattr(self, "_asdict", AttrDict)()


Base = declarative_base(cls=Base)

DbInfoColumn = lambda: Column(mutable_json_type(dbtype=JSONB, nested=True), nullable=False, default={})
DbDataColumn = lambda: Column(mutable_json_type(dbtype=JSONB, nested=True), nullable=True)


def DbInfoColumnIndex(table_name: str, column_name: str = "info"):
    return Index(
        f"{table_name}_{column_name}_idx",
        column_name,
        postgresql_using="gin"
    )


class DbPkidMixin:
    # pkid = Column(Integer, nullable=False, unique=True, primary_key=True, autoincrement=True, index=True)
    pkid = Column(Integer, Sequence("pkid_seq", metadata=Base.metadata), nullable=False, primary_key=True)


class DbDateMixin:
    date_create = Column(DateTime, default=func.now(), nullable=False, index=False)
    date_update = Column(DateTime, onupdate=func.now(), index=True)



