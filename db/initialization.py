from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

# 🔑 Replace with your actual credentials
DATABASE_URL = "postgresql://postgres:Jaibaba001#@db.mpyitncpkyunkqccefit.supabase.co:5432/postgres?sslmode=require"

# Engine (core connection)
engine = create_engine(DATABASE_URL)

# Session (used to talk to DB)
SessionLocal = sessionmaker(bind=engine)

# Base class (all ORM models inherit from this)
Base = declarative_base()