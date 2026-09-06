import csv

COLUMNS = [
    "user_id", "session_id", "action", "page", "produto_id", "categoria",
    "valor", "dispositivo", "regiao", "itens_carrinho", "event_ts",
]

CLICKSTREAM_DDL = """
CREATE TABLE clickstream (
    user_id STRING, session_id STRING, action STRING, page STRING,
    produto_id STRING, categoria STRING, valor DOUBLE,
    dispositivo STRING, regiao STRING, itens_carrinho INT,
    event_ts STRING,
    event_time AS TO_TIMESTAMP(event_ts, 'yyyy-MM-dd''T''HH:mm:ss'),
    WATERMARK FOR event_time AS event_time
) WITH (
    'connector' = 'filesystem',
    'path' = '{path}',
    'format' = 'csv'
)
"""


def make_evento(user_id, action, valor, event_ts):
    return {
        "user_id": user_id, "session_id": f"SES-{user_id}", "action": action,
        "page": "/home", "produto_id": "PROD-001", "categoria": "eletronicos",
        "valor": valor, "dispositivo": "desktop", "regiao": "Sudeste",
        "itens_carrinho": 0, "event_ts": event_ts,
    }


def register_clickstream_fixture(t_env, tmp_path, rows):
    csv_path = tmp_path / "clickstream.csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        for row in rows:
            writer.writerow([row[c] for c in COLUMNS])

    path_url = str(csv_path).replace("\\", "/")
    t_env.execute_sql(CLICKSTREAM_DDL.format(path=path_url))
