from streaming.flink_env import create_batch_env
from streaming.queries import USER_SESSIONS_SQL
from tests.helpers import make_evento, register_clickstream_fixture


def test_session_splits_on_inactivity_gap(tmp_path):
    t_env = create_batch_env()
    rows = [
        make_evento("USR-0001", "page_view", 0.0, "2026-01-01T10:00:00"),
        make_evento("USR-0001", "purchase", 100.0, "2026-01-01T10:00:10"),
        make_evento("USR-0001", "page_view", 0.0, "2026-01-01T10:00:50"),  # gap 40s > 30s
    ]
    register_clickstream_fixture(t_env, tmp_path, rows)

    with t_env.sql_query(USER_SESSIONS_SQL).execute().collect() as results:
        rows_out = list(results)

    assert len(rows_out) == 2


def test_session_does_not_mix_different_users(tmp_path):
    t_env = create_batch_env()
    rows = [
        make_evento("USR-0001", "page_view", 0.0, "2026-01-01T10:00:00"),
        make_evento("USR-0002", "page_view", 0.0, "2026-01-01T10:00:01"),
    ]
    register_clickstream_fixture(t_env, tmp_path, rows)

    with t_env.sql_query(USER_SESSIONS_SQL).execute().collect() as results:
        rows_out = list(results)

    assert len(rows_out) == 2
    assert {r[0] for r in rows_out} == {"USR-0001", "USR-0002"}
