from sqlalchemy import MetaData
from sqlalchemy.ext.declarative import declarative_base

from hydb.db.base import *

__all__ = "Base", "StatBase",


StatBase = declarative_base(metadata=MetaData(schema="stat"))
