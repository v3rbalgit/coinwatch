# src/tests/test_data_collection.py
from src.db.init_db import db_manager
from src.api.bybit_adapter import BybitAdapter
from src.services.kline_service import get_symbol_id, insert_kline_data

def test_single_symbol_collection():
    # Get one symbol and try to collect its data
    try:
        bybit = BybitAdapter()  # Create Bybit adapter instance
        symbol = bybit.get_instruments()[0]  # Get first available symbol
        print(f"Testing with symbol: {symbol}")

        with db_manager.get_session() as session:
            # Get or create symbol ID
            symbol_id = get_symbol_id(session, symbol)
            print(f"Got symbol_id: {symbol_id}")

            # Get some kline data
            kline_data = bybit.get_kline(symbol=symbol, limit=10)
            if kline_data["retCode"] == 0:
                # Format the data
                formatted_data = [
                    (int(item[0]), float(item[1]), float(item[2]), float(item[3]),
                    float(item[4]), float(item[5]), float(item[6]))
                    for item in kline_data["result"]["list"]
                ]

                # Try to insert it
                insert_kline_data(session, symbol_id, formatted_data)
                print(f"✅ Successfully inserted {len(formatted_data)} klines")
            else:
                print(f"❌ Failed to get kline data: {kline_data['retMsg']}")

    except Exception as e:
        print(f"❌ Test failed: {e}")

if __name__ == "__main__":
    test_single_symbol_collection()