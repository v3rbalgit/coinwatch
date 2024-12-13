from sqlalchemy import MetaData
from sqlalchemy.orm import declarative_base

# Define naming convention
convention = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s"
}

# Create base classes for each schema
market_data_metadata = MetaData(schema="market_data", naming_convention=convention)
MarketDataBase = declarative_base(metadata=market_data_metadata)

fundamental_data_metadata = MetaData(schema="fundamental_data", naming_convention=convention)
FundamentalDataBase = declarative_base(metadata=fundamental_data_metadata)

monitor_metadata = MetaData(schema="monitor", naming_convention=convention)
MonitorBase = declarative_base(metadata=monitor_metadata)

# Combined metadata for migrations
combined_metadata = MetaData()
for meta in [market_data_metadata, fundamental_data_metadata, monitor_metadata]:
    for table in meta.tables.values():
        table.tometadata(combined_metadata)
