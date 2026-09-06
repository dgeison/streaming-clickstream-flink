METRICS_WINDOW_SQL = """
SELECT window_start, window_end,
       COUNT(*) AS total_eventos,
       COUNT(DISTINCT user_id) AS usuarios_ativos,
       SUM(CASE WHEN action = 'page_view' THEN 1 ELSE 0 END) AS page_views,
       SUM(CASE WHEN action = 'click' THEN 1 ELSE 0 END) AS clicks,
       SUM(CASE WHEN action = 'add_to_cart' THEN 1 ELSE 0 END) AS add_to_carts,
       SUM(CASE WHEN action = 'purchase' THEN 1 ELSE 0 END) AS purchases,
       CAST(ROUND(SUM(CASE WHEN action = 'purchase' THEN valor ELSE 0 END), 2) AS DOUBLE) AS receita
FROM TABLE(TUMBLE(TABLE clickstream, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))
GROUP BY window_start, window_end
"""

USER_SESSIONS_SQL = """
WITH ordered AS (
    SELECT *,
           LAG(event_time) OVER (PARTITION BY user_id ORDER BY event_time) AS prev_event_time
    FROM clickstream
),
flagged AS (
    SELECT *,
           CASE
               WHEN prev_event_time IS NULL
                    OR TIMESTAMPDIFF(SECOND, prev_event_time, event_time) > 30
               THEN 1 ELSE 0
           END AS is_new_session
    FROM ordered
),
sessioned AS (
    SELECT *,
           SUM(is_new_session) OVER (PARTITION BY user_id ORDER BY event_time) AS session_num
    FROM flagged
)
SELECT user_id,
       MIN(event_time) AS sessao_inicio,
       MAX(event_time) AS sessao_fim,
       COUNT(*) AS acoes_total,
       SUM(CASE WHEN action = 'purchase' THEN 1 ELSE 0 END) AS compras,
       CAST(ROUND(SUM(CASE WHEN action = 'purchase' THEN valor ELSE 0 END), 2) AS DOUBLE) AS receita_sessao
FROM sessioned
GROUP BY user_id, session_num
"""
