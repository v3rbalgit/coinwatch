# src/tests/test_connections.py
from sqlalchemy import text  # Add this import
from src.db.init_db import db_manager
from src.api.bybit_adapter import bybit

def test_connections():
    try:
        with db_manager.get_session() as session:
            session.execute(text("SELECT 1"))  # Fix the text() usage
        print("✅ Database connection successful")
    except Exception as e:
        print(f"❌ Database connection failed: {e}")

    try:
        symbols = bybit.get_instruments()
        print(f"✅ Bybit API connection successful. Found {len(symbols)} symbols")
    except Exception as e:
        print(f"❌ Bybit API connection failed: {e}")
