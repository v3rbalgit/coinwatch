# tests/test_basic.py
import pytest

from src.core.models import KlineData
from decimal import Decimal

def test_basic_imports():
    """Test that basic imports work"""
    kline = KlineData(
        timestamp=1635724800000,
        open_price=Decimal("50000.00"),
        high_price=Decimal("51000.00"),
        low_price=Decimal("49000.00"),
        close_price=Decimal("50500.00"),
        volume=Decimal("10.5"),
        turnover=Decimal("525000.00"),
        symbol="BTCUSDT",
        timeframe="5m"
    )
    assert kline.symbol == "BTCUSDT"

# @pytest.mark.sqlite
# async def test_sqlite_connection(sqlite_db):
#     async with sqlite_db.transaction() as session:
#         result = await session.execute("SELECT 1")
#         assert result.scalar() == 1

# @pytest.mark.mysql
# async def test_mysql_connection(mysql_db):
#     async with mysql_db.transaction() as session:
#         result = await session.execute("SELECT 1")
#         assert result.scalar() == 1