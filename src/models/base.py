# src/models/base.py

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

# Create base class with naming convention
Base = declarative_base(metadata=MetaData(schema="public", naming_convention=convention))