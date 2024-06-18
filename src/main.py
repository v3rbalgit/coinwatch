from time import sleep

from sqlalchemy.orm import sessionmaker
from db.init_db import init_db, engine
from api.bybit_adapter import get_kline, get_instruments
from services.kline_service import get_symbol_id, get_latest_timestamp, insert_kline_data
from utils.timestamp import get_current_timestamp, get_past_timestamp, calculate_hours_between


def main():
    init_db()  # Initialize the database and create tables if needed

    Session = sessionmaker(bind=engine)
    session = Session()

    symbols = get_instruments()
    end_time = get_current_timestamp()

    for symbol in symbols:
        symbol_id = get_symbol_id(session, symbol)
        latest_timestamp = get_latest_timestamp(session, symbol_id)

        if latest_timestamp:
            start_time = latest_timestamp
        else:
            start_time = get_past_timestamp(2)

        hours_between = calculate_hours_between(start_time, end_time)

        if (hours_between == 0):
            continue

        kline_data = get_kline(symbol, hours_between)

        if kline_data["retCode"] == 0:
            kline_list = kline_data["result"]["list"]
            formatted_data = [
                (
                    int(item[0]),
                    float(item[1]),
                    float(item[2]),
                    float(item[3]),
                    float(item[4]),
                    float(item[5]),
                    float(item[6])
                ) for item in kline_list
            ]

            print(f"Added last {hours_between} hours - {symbol}")

            insert_kline_data(session, symbol_id, formatted_data)

    session.close()


if __name__ == "__main__":
    sleep(5) # FIXME! Ensure database started
    main()