from sqlalchemy import Column, String, BigInteger
from models.base import Base


class Symbol(Base):
    __tablename__ = 'symbols'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False, unique=True)