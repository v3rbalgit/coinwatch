import pandas as pd
import pandas_ta as ta
import numpy as np
from db.init_db import engine
from sqlalchemy import MetaData,Table,select,Engine

# Rovnaka funkcia je v kline_service
def get_symbol_id(eng: Engine, sym: str ="BTCUSDT") -> int:   # FIXME: error checking, make sym required
  """
  Fetches id of a symbol from the database

  Args:
  eng (Engine): Database engine to use
  sym (str): Symbol name, e.g. 'BTCUSDT'

  Returns:
  int: id of the symbol
  """

  metadata = MetaData()
  symbols = Table('symbols', metadata, autoload_with=eng)

  with eng.connect() as conn:
    res = conn.execute(select(symbols).where(symbols.c.name == sym))
    rows = res.fetchall()

  return rows[0][0]

id = get_symbol_id(engine, 'ETHUSDT')

prices = pd.read_sql(f"SELECT * FROM kline_data WHERE symbol_id = {id}", engine)
prices['start_time'] = pd.to_datetime(prices['start_time'], unit='ms')
prices.set_index('start_time', inplace=True)
prices.ta.rsi(close='close_price', append=True)

rsi_1h = prices['RSI_14'].iloc[-1]
del prices['RSI_14']

# prices_4h = prices['close_price'].resample('4h').last().reset_index()
# prices_4h.ta.rsi(close='close_price', append=True)
# rsi_4h = prices_4h['RSI_14'].iloc[-1]


# 4h funguje zvlastne, berie do uvahy aj posledne 1h sviece, ktore este nezavreli
prices_4h = prices.resample('4h').agg({
  'symbol_id': 'last',
  'open_price': 'first',
  'high_price': 'max',
  'low_price': 'min',
  'close_price': 'last',
  'volume': 'sum',
  'turnover': 'sum'
}).reset_index()
prices_4h.ta.rsi(close='close_price', append=True)
rsi_4h = prices_4h['RSI_14'].iloc[-1]

# Skript vypluva divne cenove hodnoty pocas toho ako bezi
print(prices.tail(10))
print(f'\nLatest ETHUSDT 1h RSI: {rsi_1h}\n')
print(prices_4h.tail())
print(f'\nLatest ETHUSDT 4h RSI: {rsi_4h}\n')