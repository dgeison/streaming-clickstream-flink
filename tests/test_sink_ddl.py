def test_ensure_tables_creates_expected_tables(clean_db):
    with clean_db.cursor() as cur:
        cur.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
        )
        tables = {row["table_name"] for row in cur.fetchall()}
    assert {"metricas_por_janela", "sessoes_usuario"}.issubset(tables)
