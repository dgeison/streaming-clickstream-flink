import os

import psycopg2
from psycopg2.extras import RealDictCursor


def get_connection():
    return psycopg2.connect(
        host=os.environ.get("STREAMING_DB_HOST", "localhost"),
        port=os.environ.get("STREAMING_DB_PORT", "5434"),
        dbname=os.environ.get("STREAMING_DB_NAME", "streaming"),
        user=os.environ.get("STREAMING_DB_USER", "stream"),
        password=os.environ.get("STREAMING_DB_PASSWORD", "stream"),
        cursor_factory=RealDictCursor,
    )
