from __future__ import annotations
from pybit.unified_trading import HTTP
from datetime import datetime
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from typing import List


db = create_engine("sqlite:///DATA.db")
session = HTTP()


def get_instruments() -> List[str]:
  symbols = []
  res = session.get_instruments_info(category='linear')

  try:
    if res['retMsg'] == 'OK':
      symbols.extend(symbol['symbol'] for symbol in res['result']['list'] if 'USDT' in symbol['symbol'])
    else:
      raise ValueError(f'Could not fetch results: {res["retCode"]} {res["retMsg"]}')
  except ValueError as e:
    print(e.args[0])

  return symbols


def get_prices(symbols: List):
  prices = {}

  for symbol in symbols:
    # TODO: skontrolovat ake najnovsie data sa nachadzaju v databaze,
    # porovnat s aktualnymi a premazat alebo doplnit
    # TODO: async ziskavanie dat
    res = session.get_kline(category="linear", interval='60', symbol=symbol, limit=720) # 30 days of 1h price data

    try:
      if res['retMsg'] == 'OK':
        timestamps = [pd.to_datetime(datetime.fromtimestamp(int(kline[0]) / 1000)).isoformat() for kline in res['result']['list']]  # prepare datetime series from kline timestamps for indexing

        prices[symbol] = pd.DataFrame([{
          'open': kline[1],
          'high': kline[2],
          'low': kline[3],
          'close': kline[4],
          'volume': kline[5],
          'turnover': kline[6]
        } for kline in res['result']['list']], index=timestamps, dtype=np.float32)
      else:
        raise ValueError(f'Could not fetch results: {res["retCode"]} {res["retMsg"]}')
    except ValueError as e:
      print(e.args[0])


  return prices

if __name__ == '__main__':
  symbols = get_instruments() # trva dlho
  print(symbols)
  prices = get_prices(symbols)
  print(prices['BTCUSDT'].head())
