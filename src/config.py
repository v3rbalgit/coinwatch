# Should be passed from env
DB_CONFIG = {
    'user': 'user',
    'password': 'password',
    'host': 'db',
    'database': 'coinwatch',
}

DATABASE_URL = f"mysql+mysqlconnector://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}/{DB_CONFIG['database']}"

SYNCHRONIZATION_DAYS = 7
