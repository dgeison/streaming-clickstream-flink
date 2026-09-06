DDL = """
CREATE TABLE IF NOT EXISTS metricas_por_janela (
    janela_inicio TIMESTAMP NOT NULL,
    janela_fim TIMESTAMP NOT NULL,
    total_eventos BIGINT NOT NULL,
    usuarios_ativos BIGINT NOT NULL,
    page_views BIGINT NOT NULL,
    clicks BIGINT NOT NULL,
    add_to_carts BIGINT NOT NULL,
    purchases BIGINT NOT NULL,
    receita DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (janela_inicio, janela_fim)
);
CREATE TABLE IF NOT EXISTS sessoes_usuario (
    user_id TEXT NOT NULL,
    sessao_inicio TIMESTAMP NOT NULL,
    sessao_fim TIMESTAMP NOT NULL,
    acoes_total BIGINT NOT NULL,
    compras BIGINT NOT NULL,
    receita_sessao DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (user_id, sessao_inicio)
);
"""

DROP_ALL = """
DROP TABLE IF EXISTS metricas_por_janela, sessoes_usuario CASCADE;
"""


def ensure_tables(conn):
    with conn.cursor() as cur:
        cur.execute(DDL)
    conn.commit()
