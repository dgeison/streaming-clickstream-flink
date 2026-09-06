from streaming.flink_env import create_batch_env
from streaming.queries import METRICS_WINDOW_SQL
from tests.helpers import make_evento, register_clickstream_fixture


def test_metrics_window_aggregates_within_a_single_window(tmp_path):
    t_env = create_batch_env()
    rows = [
        make_evento("USR-0001", "page_view", 0.0, "2026-01-01T10:00:05"),
        make_evento("USR-0001", "purchase", 199.90, "2026-01-01T10:00:10"),
        make_evento("USR-0002", "page_view", 0.0, "2026-01-01T10:00:20"),
    ]
    register_clickstream_fixture(t_env, tmp_path, rows)

    with t_env.sql_query(METRICS_WINDOW_SQL).execute().collect() as results:
        rows_out = list(results)

    assert len(rows_out) == 1
    row = rows_out[0]
    assert row[2] == 3          # total_eventos
    assert row[3] == 2          # usuarios_ativos
    assert row[8] == 199.90     # receita


def test_metrics_window_does_not_leak_events_across_window_boundary(tmp_path):
    t_env = create_batch_env()
    rows = [
        make_evento("USR-0001", "purchase", 100.0, "2026-01-01T10:00:59"),
        make_evento("USR-0001", "purchase", 50.0, "2026-01-01T10:01:00"),
    ]
    register_clickstream_fixture(t_env, tmp_path, rows)

    with t_env.sql_query(METRICS_WINDOW_SQL).execute().collect() as results:
        rows_out = sorted(results, key=lambda r: r[0])

    assert len(rows_out) == 2
    assert rows_out[0][8] == 100.0
    assert rows_out[1][8] == 50.0
