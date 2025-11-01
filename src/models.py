"""
SQLAlchemy ORM models for matches and a small metadata table.
"""
from sqlalchemy import (
    Column,
    Integer,
    String,
    Date,
    DateTime,
    UniqueConstraint,
    func,
)
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Match(Base):
    __tablename__ = "matches"
    id = Column(Integer, primary_key=True, index=True)
    date = Column(Date, nullable=False, index=True)
    home_team = Column(String, nullable=False, index=True)
    away_team = Column(String, nullable=False, index=True)
    home_score = Column(Integer, nullable=True)
    away_score = Column(Integer, nullable=True)
    league = Column(String, nullable=True, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        UniqueConstraint("date", "home_team", "away_team", name="uix_date_home_away"),
    )


class Meta(Base):
    """
    small key/value table for storing counters, last-import timestamps etc.
    """
    __tablename__ = "meta"
    key = Column(String, primary_key=True)
    value = Column(String, nullable=True)
