import pandas as pd
from db.init_db import engine


df = pd.read_sql(sql="SELECT * FROM kline_data WHERE symbol_id = 1",con=engine)
print(df.head())