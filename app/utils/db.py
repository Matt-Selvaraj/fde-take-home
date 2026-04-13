import datetime
import uuid

from sqlalchemy import create_engine, Column, String, Integer, DateTime, JSON, UniqueConstraint
from sqlalchemy.orm import sessionmaker, declarative_base

from app.utils.config import settings

Base = declarative_base()


class Run(Base):
    __tablename__ = "runs"
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    status = Column(String)  # succeeded, failed
    month = Column(String)
    source_uri = Column(String)
    rows_scanned = Column(Integer, default=0)
    alerts_sent = Column(Integer, default=0)
    skipped_replay = Column(Integer, default=0)
    failed_deliveries = Column(Integer, default=0)
    duplicates_found = Column(Integer, default=0)
    errors = Column(JSON, default=list)
    created_at = Column(DateTime, default=datetime.datetime.now(datetime.timezone.utc))


class AlertOutcome(Base):
    __tablename__ = "alert_outcomes"
    id = Column(Integer, primary_key=True)
    account_id = Column(String)
    month = Column(String)
    alert_type = Column(String, default="at_risk")
    channel = Column(String)
    status = Column(String)  # sent, failed, skipped_replay, unknown_region
    sent_at = Column(DateTime)
    error = Column(String)

    __table_args__ = (
        UniqueConstraint('account_id', 'month', 'alert_type', name='uix_account_month_type'),
    )


engine = create_engine(settings.DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def init_db():
    Base.metadata.create_all(bind=engine)
