from streaming.flink_env import create_streaming_env, create_batch_env


def test_create_batch_env_can_run_a_simple_query():
    t_env = create_batch_env()
    with t_env.sql_query("SELECT 1 AS um").execute().collect() as results:
        rows = list(results)
    assert rows[0][0] == 1


def test_create_streaming_env_sets_parallelism_two():
    t_env = create_streaming_env()
    assert t_env.get_config().get("parallelism.default", None) == "2"


def test_create_batch_env_sets_parallelism_one():
    t_env = create_batch_env()
    assert t_env.get_config().get("parallelism.default", None) == "1"
