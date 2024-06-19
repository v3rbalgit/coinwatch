from typing import List

from pybit.unified_trading import HTTP

def get_kline(symbol, limit):
    session = HTTP()

    return session.get_kline(category="linear", interval='60', symbol=symbol, limit=limit)

def get_instruments() -> List[str]:
    session = HTTP()
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