from sqlalchemy import create_engine, MetaData

db = create_engine("sqlite:///DATA.db")

meta = MetaData()

