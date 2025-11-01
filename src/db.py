"""
DB helpers: engine and session factory. Default DB: sqlite:///data/matches.db
"""
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

DATABASE_URL = os.environ.get("MATCHES_DATABASE_URL", "sqlite:///data/matches.db")

# For sqlite, allow multithreaded access
connect_args = {}
if DATABASE_URL.startswith("sqlite"):
    connect_args = {"check_same_thread": False}

engine = create_engine(DATABASE_URL, connect_args=connect_args, future=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine, future=True)


def init_db():
    """
    Create tables (if they do not exist).
    """
    from src.models import Base

    # import triggers or other DB initializers here if needed
    Base.metadata.create_all(bind=engine)
