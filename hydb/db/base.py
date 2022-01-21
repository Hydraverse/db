from attrdict import AttrDict
from sqlalchemy import Column, DateTime, func, Integer, Sequence, Index
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import declarative_base
from sqlalchemy_json import mutable_json_type

__all__ = (
    "Base",
    "DbPkidColumn", "DbDateCreateColumn", "DbDateUpdateColumn",
    "DbInfoColumn", "DbDataColumn",
    "DbInfoColumnIndex",
)


class Base:
    __mapper_args__ = {"eager_defaults": True}

    def asdict(self) -> AttrDict:
        return getattr(self, "_asdict", AttrDict)()


Base = declarative_base(cls=Base)

DbInfoColumn = lambda default=...: Column(mutable_json_type(dbtype=JSONB, nested=True), nullable=False, default={} if default is ... else default)
DbDataColumn = lambda default=None: Column(mutable_json_type(dbtype=JSONB, nested=True), nullable=True, default=default)


def DbInfoColumnIndex(table_name: str, column_name: str = "info"):
    return Index(
        f"{table_name}_{column_name}_idx",
        column_name,
        postgresql_using="gin"
    )


DbPkidColumn = lambda seq="pkid_seq": Column(
    Integer, Sequence(seq, metadata=Base.metadata), nullable=False, primary_key=True, unique=True
)


DbDateCreateColumn = lambda: Column(DateTime, default=func.now(), nullable=False, index=False)
DbDateUpdateColumn = lambda: Column(DateTime, onupdate=func.now(), index=True)
