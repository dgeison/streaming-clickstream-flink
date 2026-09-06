from streaming.queries import METRICS_WINDOW_SQL, USER_SESSIONS_SQL_STREAMING

CLICKSTREAM_SOURCE_DDL = """
CREATE TABLE clickstream (
    user_id STRING, session_id STRING, action STRING, page STRING,
    produto_id STRING, categoria STRING, valor DOUBLE,
    dispositivo STRING, regiao STRING, itens_carrinho INT,
    `timestamp` STRING,
    event_time AS TO_TIMESTAMP(`timestamp`, 'yyyy-MM-dd''T''HH:mm:ss'),
    WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'clickstream',
    'properties.bootstrap.servers' = '{bootstrap_servers}',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
)
"""

METRICS_SINK_DDL = """
CREATE TABLE metricas_por_janela (
    janela_inicio TIMESTAMP(3), janela_fim TIMESTAMP(3),
    total_eventos BIGINT, usuarios_ativos BIGINT,
    page_views BIGINT, clicks BIGINT, add_to_carts BIGINT,
    purchases BIGINT, receita DOUBLE
) WITH (
    'connector' = 'jdbc',
    'url' = '{jdbc_url}',
    'table-name' = 'metricas_por_janela',
    'username' = '{username}',
    'password' = '{password}'
)
"""

SESSIONS_SINK_DDL = """
CREATE TABLE sessoes_usuario (
    user_id STRING, sessao_inicio TIMESTAMP(3), sessao_fim TIMESTAMP(3),
    acoes_total BIGINT, compras BIGINT, receita_sessao DOUBLE
) WITH (
    'connector' = 'jdbc',
    'url' = '{jdbc_url}',
    'table-name' = 'sessoes_usuario',
    'username' = '{username}',
    'password' = '{password}'
)
"""


def registrar_tabelas(t_env, bootstrap_servers, jdbc_url, username, password):
    t_env.execute_sql(CLICKSTREAM_SOURCE_DDL.format(bootstrap_servers=bootstrap_servers))
    t_env.execute_sql(METRICS_SINK_DDL.format(jdbc_url=jdbc_url, username=username, password=password))
    t_env.execute_sql(SESSIONS_SINK_DDL.format(jdbc_url=jdbc_url, username=username, password=password))


def rodar_jobs(t_env):
    stmt_set = t_env.create_statement_set()
    stmt_set.add_insert_sql(f"INSERT INTO metricas_por_janela {METRICS_WINDOW_SQL}")
    stmt_set.add_insert_sql(f"INSERT INTO sessoes_usuario {USER_SESSIONS_SQL_STREAMING}")
    return stmt_set.execute()
