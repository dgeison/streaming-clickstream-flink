import pytest

from streaming.db import get_connection
from streaming.sink_ddl import DROP_ALL, ensure_tables


@pytest.fixture
def clean_db():
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(DROP_ALL)
    conn.commit()
    ensure_tables(conn)
    yield conn
    conn.close()
