import pandas as pd
import pandas_ta as ta
from db.init_db import engine
from sqlalchemy import MetaData,Table,select,Engine


def get_symbols(eng: Engine):
  """
  Fetches all symbols from the database

  Args:
  eng (Engine): Database engine to use

  Returns:
  Any: list of tuples
  """

  metadata = MetaData()
  symbols = Table('symbols', metadata, autoload_with=eng)

  with eng.connect() as conn:
    res = conn.execute(select(symbols))
    rows = res.fetchall()

  return rows

symbols = get_symbols(engine)

def calc_rsi_1h():
  rsi_1h = []

  for symbol in symbols:
    prices = pd.read_sql(f"SELECT * FROM kline_data WHERE symbol_id = {symbol[0]}", engine)
    prices['start_time'] = pd.to_datetime(prices['start_time'], unit='ms')
    prices.set_index('start_time', inplace=True)
    prices.ta.rsi(close='close_price', append=True)
    rsi_1h.append((symbol[1],prices['RSI_14'].iloc[-1]))

  return rsi_1h

def calc_rsi_4h():
  rsi_4h = []

  for symbol in symbols:
    prices = pd.read_sql(f"SELECT * FROM kline_data WHERE symbol_id = {symbol[0]}", engine)
    prices['start_time'] = pd.to_datetime(prices['start_time'], unit='ms')
    prices.set_index('start_time', inplace=True)
    prices_4h = prices['close_price'].resample('4h').last().reset_index()
    prices_4h.ta.rsi(close='close_price', append=True)
    rsi_4h.append((symbol[1],prices['RSI_14'].iloc[-1]))

  return rsi_4h


# prices_4h = prices['close_price'].resample('4h').last().reset_index()
# prices_4h.ta.rsi(close='close_price', append=True)
# rsi_4h = prices_4h['RSI_14'].iloc[-1]


# 4h funguje zvlastne, berie do uvahy aj posledne 1h sviece, ktore este nezavreli
# prices_4h = prices.resample('4h').agg({
#   'symbol_id': 'last',
#   'open_price': 'first',
#   'high_price': 'max',
#   'low_price': 'min',
#   'close_price': 'last',
#   'volume': 'sum',
#   'turnover': 'sum'
# }).reset_index()
# prices_4h.ta.rsi(close='close_price', append=True)
# rsi_4h = prices_4h['RSI_14'].iloc[-1]

# Skript vypluva divne cenove hodnoty pocas toho ako bezi
print('Sorted 1H RSI coins:')
print(sorted(calc_rsi_1h(), key=lambda x: x[1], reverse=True))
print('Sorted 4H RSI coins:')
print(sorted(calc_rsi_4h(), key=lambda x: x[1], reverse=True))