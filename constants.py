import os

DB_CONFIG = {
    'user': 'root',
    'password': os.getenv('DB_PASS'),
    'host': '127.0.0.1',
    'database': 'covid_data',
    'raise_on_warnings': True
}
