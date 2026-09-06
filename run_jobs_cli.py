import os

from streaming.db import get_connection
from streaming.flink_env import create_streaming_env
from streaming.jobs import registrar_tabelas, rodar_jobs
from streaming.sink_ddl import ensure_tables


def main():
    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    db_host = os.environ.get("STREAMING_DB_HOST", "postgres")
    db_port = os.environ.get("STREAMING_DB_PORT", "5432")
    db_name = os.environ.get("STREAMING_DB_NAME", "streaming")
    db_user = os.environ.get("STREAMING_DB_USER", "stream")
    db_password = os.environ.get("STREAMING_DB_PASSWORD", "stream")

    conn = get_connection()
    ensure_tables(conn)
    conn.close()

    jdbc_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"

    t_env = create_streaming_env()
    registrar_tabelas(t_env, bootstrap_servers, jdbc_url, db_user, db_password)
    resultado = rodar_jobs(t_env)
    print("Jobs Flink em execucao (metricas por janela + sessoes de usuario)...")
    resultado.wait()


if __name__ == "__main__":
    main()
