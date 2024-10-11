from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError
from config import DATABASE_URL
from models.base import Base
import time
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_db_engine(max_retries=5, retry_interval=10):
    for attempt in range(max_retries):
        try:
            engine = create_engine(DATABASE_URL, echo=False, pool_pre_ping=True)
            # Test the connection
            with engine.connect() as connection:
                connection.execute(text("SELECT 1"))
            print("Database connection established successfully.")
            return engine
        except OperationalError as e:
            if attempt < max_retries - 1:
                print(f"Connection attempt {attempt + 1} failed. Retrying in {retry_interval} seconds...")
                time.sleep(retry_interval)
            else:
                print("Failed to connect to the database after multiple attempts.")
                raise

engine = create_db_engine()
Session = sessionmaker(bind=engine)

def init_db():
    try:
        Base.metadata.create_all(engine)
        logger.info("Database tables created successfully.")
    except Exception as e:
        logger.error(f"Error creating database tables: {str(e)}")
        raise