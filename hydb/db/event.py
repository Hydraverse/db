from __future__ import annotations

from typing import List, Optional

from datetime import datetime, timedelta

from hydra import log
from sqlalchemy import Column, String, DateTime, func, event as sa_event, and_, Text, not_

from .base import *
from .db import DB

__all__ = "Event",


class Event(Base):
    __tablename__ = "event"
    __table_args__ = (
        DbInfoColumnIndex(__tablename__, "claim"),
    )

    __LISTENERS = []

    pkid = DbPkidColumn(seq="event_seq")
    date_create = DbDateCreateColumn()
    date_expire = Column(DateTime, nullable=False, default=lambda: datetime.now() + timedelta(hours=18))
    claim = DbInfoColumn(default=[])
    event = Column(String, nullable=False)
    data = Column(Text, nullable=False)

    @staticmethod
    def insert_listener_add(callback):
        Event.__LISTENERS.append(callback)

    @staticmethod
    def insert_listener_rem(callback):
        Event.__LISTENERS.remove(callback)

    @staticmethod
    def insert_listener_call(session, target):
        for callback in Event.__LISTENERS:
            # noinspection PyBroadException
            try:
                callback(session, target)
            except BaseException as exc:
                log.critical("Event insert listener callback error", exc_info=exc)

        Event.delete_expired(session)

    def claim_for(self, claimant: str) -> bool:
        if claimant in self.claim:
            return False

        self.claim.append(claimant)
        return True

    @staticmethod
    def claim_all_for(db: DB, event: str, claimant: str, limit: Optional[int] = None) -> List[Event]:
        q = db.Session.query(
            Event
        ).filter(
            and_(
                Event.event == event,
                not_(
                    Event.claim.op("?")(claimant)
                )
            )
        ).order_by(
            Event.pkid
        )

        if limit is not None:
            q = q.limit(limit)

        events: List[Event] = q.all()

        if len(events):
            for event in events:
                event.claim_for(claimant)

            db.Session.commit()

            for event in events:
                db.Session.refresh(event)

        return events

    @staticmethod
    def unclaimed_for(db: DB, claimant: str) -> List[Event]:
        return db.Session.query(
            Event
        ).filter(
            not_(
                Event.claim.op("?")(claimant)
            )
        ).order_by(
            Event.pkid
        ).all()

    @staticmethod
    def delete_expired(session):
        deleted: int = session.query(
            Event
        ).filter(
            func.now() >= Event.date_expire
        ).delete(
            synchronize_session="fetch"
        )

        if deleted:
            log.info(f"Deleted {deleted} expired events.")
            # Causes later error "This transaction is closed":
            # session.commit()


@sa_event.listens_for(Event, 'after_insert')
def event_after_insert(mapper, connection, target):
    if target is None:
        log.warning("event_after_insert(): target is None.")
        return

    @sa_event.listens_for(DB.current_session(), 'after_flush', once=True)
    def event_after_insert_flush(session, flush_context):
        Event.insert_listener_call(session, target)
