import os
import logging
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timezone


load_dotenv()
logging.getLogger().setLevel(logging.INFO)


dsl = {'dbname': os.environ.get('DB_NAME'), 'user': os.environ.get('DB_USER'), 'password': os.environ.get('DB_PASSWORD'), 'host': os.environ.get('DB_HOST'), 'port': os.environ.get('DB_PORT')}

with psycopg2.connect(**dsl, cursor_factory=RealDictCursor) as pg_conn:
    with pg_conn.cursor() as cursor:

        cursor.execute(f"""
            update content.film_work 
            set updated_at = '{datetime(2023, 3, 16, 20, 14, 9, 221855, tzinfo=timezone.utc)}'::timestamp
            where id = '3d825f60-9fff-4dfe-b294-1a45fa1e115d';
        """)

        print(cursor.rowcount)

        pg_conn.commit()