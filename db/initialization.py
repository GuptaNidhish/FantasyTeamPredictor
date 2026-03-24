from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

# 🔑 Replace with your actual credentials
DATABASE_URL = "postgresql+psycopg2://postgres:Jaibaba001#@localhost:5432/fantasy_db"

# Engine (core connection)
engine = create_engine(DATABASE_URL)

# Session (used to talk to DB)
SessionLocal = sessionmaker(bind=engine)

# Base class (all ORM models inherit from this)
Base = declarative_base()