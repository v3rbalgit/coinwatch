from sqlalchemy.orm import Session
from models.symbol import Symbol
from models.kline import Kline

def get_symbol_id(session: Session, symbol_name: str) -> int:
    symbol = session.query(Symbol).filter_by(name=symbol_name).first()
    if not symbol:
        symbol = Symbol(name=symbol_name)
        session.add(symbol)
        session.commit()
        session.refresh(symbol)
    return symbol.id

def get_latest_timestamp(session: Session, symbol_id: int):
    result = session.query(Kline.start_time).filter_by(symbol_id=symbol_id).order_by(Kline.start_time.desc()).first()
    return result[0] if result else None

def insert_kline_data(session: Session, symbol_id: int, kline_data):
    for data in kline_data:
        kline_entry = Kline(
            symbol_id=symbol_id,
            start_time=data[0],
            open_price=data[1],
            high_price=data[2],
            low_price=data[3],
            close_price=data[4],
            volume=data[5],
            turnover=data[6]
        )
        session.merge(kline_entry)  # Use merge to handle duplicates
    session.commit()
