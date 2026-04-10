import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

# Load .env for local
try:
    from dotenv import load_dotenv
    load_dotenv()
except:
    pass

# Try local .env first
DATABASE_URL = os.getenv("DATABASE_URL")

# If not found, try Streamlit secrets (for deployment)
if not DATABASE_URL:
    try:
        import streamlit as st
        DATABASE_URL = st.secrets["DATABASE_URL"]
    except:
        raise Exception("DATABASE_URL not found in environment variables or Streamlit secrets")

# Engine
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_recycle = 300
)

# Session
SessionLocal = sessionmaker(bind=engine)

# Base
Base = declarative_base()