# Arquitetura

## Módulos

- **`streaming/events.py`**: gerador determinístico de eventos de
  clickstream. `new_user_state(rng)` cria o estado inicial de um usuário
  (dispositivo, região, carrinho vazio); `atualizar_carrinho(estado,
  action, produto)` muta o carrinho in-place conforme a ação
  (`add_to_cart` empilha, `purchase` esvazia, `remove_cart` desempilha);
  `generate_event(user_id, estado, rng, timestamp)` sorteia a próxima ação
  via uma cadeia de Markov simples (`TRANSICOES`, pesos por ação anterior)
  e retorna o dict do evento. Tudo recebe `rng`/`timestamp` como parâmetro
  em vez de usar `random`/`datetime.now()` global — é isso que torna
  `generate_event` determinístico com a mesma seed, verificado em
  `tests/test_events.py`.

- **`streaming/flink_env.py`**: duas factories de `TableEnvironment`.
  `create_streaming_env()` roda em modo streaming, `parallelism.default=2`,
  e configura `pipeline.jars` apontando para os 3 JARs de conector
  (`flink-sql-connector-kafka.jar`, `flink-connector-jdbc.jar`,
  `postgresql-driver.jar`) baixados em `/app/lib` no build da imagem — é a
  usada por `run_jobs_cli.py` em produção. `create_batch_env()` roda em
  modo batch, `parallelism.default=1`, sem nenhum JAR configurado — os
  testes bounded não tocam Kafka nem Postgres via Flink (só filesystem/CSV),
  então não precisam desses conectores.

- **`streaming/queries.py`**: as duas queries de agregação por janela, como
  texto SQL puro — nenhuma delas sabe de onde vêm os dados, só assumem que
  existe uma tabela `clickstream` com uma coluna `event_time` (rowtime com
  watermark) já registrada. `METRICS_WINDOW_SQL` é a janela TUMBLE de 1
  minuto. Sessão de usuário tem **duas** constantes, não uma —
  `USER_SESSIONS_SQL_STREAMING` e `USER_SESSIONS_SQL_BATCH` — explicado na
  seção dedicada abaixo.

- **`streaming/db.py` / `streaming/sink_ddl.py`**: `db.py` abre a conexão
  Postgres (`get_connection()`, lendo `STREAMING_DB_*` de env vars com
  defaults locais). `sink_ddl.py` guarda o DDL das duas tabelas de destino
  (`metricas_por_janela`, `sessoes_usuario`, cada uma com `PRIMARY KEY`
  própria) e `ensure_tables(conn)`, que roda o DDL e commita — chamado tanto
  por `run_jobs_cli.py` antes de subir o job Flink quanto pela fixture
  `clean_db` dos testes (`tests/conftest.py`), que dropa e recria as
  tabelas a cada teste para isolamento.

- **`streaming/producer.py`**: `publicar_clickstream(bootstrap_servers,
  duracao_segundos, seed=None, sleep_fn=time.sleep)` — loop que sorteia um
  usuário, gera o próximo evento dele via `streaming.events` e publica no
  tópico `clickstream` (`confluent_kafka.Producer`, `acks=all`), dormindo
  entre 0.1s e 0.4s (via `sleep_fn`, injetável para testes) entre eventos.
  Retorna o total de eventos enviados. Não tem teste automatizado dedicado
  (decisão de escopo do plano) — publica em Kafka real, verificado
  manualmente na verificação end-to-end.

- **`streaming/jobs.py`**: a camada que liga tudo. `CLICKSTREAM_SOURCE_DDL`
  declara a tabela Kafka `clickstream` como source (`scan.startup.mode =
  'latest-offset'`, watermark de 10s — ver `docs/conceitos_streaming.md`);
  `METRICS_SINK_DDL`/`SESSIONS_SINK_DDL` declaram as duas tabelas JDBC como
  sink. `registrar_tabelas(t_env, bootstrap_servers, jdbc_url, username,
  password)` registra as três tabelas no `TableEnvironment`.
  `rodar_jobs(t_env)` monta um único `StatementSet` com dois
  `INSERT INTO` (métricas e sessões) e roda os dois como um job Flink só —
  `resultado.wait()` (chamado em `run_jobs_cli.py`) bloqueia até o job
  terminar ou ser interrompido.

## Por que `queries.py` foi extraído como módulo compartilhado

`METRICS_WINDOW_SQL` é a mesma string usada tanto por `streaming/jobs.py`
(produção, contra Kafka real em modo streaming) quanto pelos testes
bounded em `tests/test_metrics_window.py` (modo batch, contra um CSV
fixo). Extrair a query para um módulo próprio, sem nenhuma dependência de
Kafka/Postgres/Flink runtime, foi o que tornou isso possível: o teste não
está verificando "uma query parecida" com a de produção, está rodando
**exatamente** a mesma string SQL de produção contra uma fonte de dados
controlada e determinística. Se alguém editar a lógica de agregação em
produção sem tocar no teste (ou vice-versa), o teste denuncia a
divergência na próxima execução — é o mesmo motivo pelo qual, quando a
sessão de usuário precisou de sintaxes diferentes para streaming e batch
(próxima seção), a solução foi criar uma segunda constante em vez de
reescrever a query direto dentro de `jobs.py`: manter a superfície de
"contrato testável" centralizada num único arquivo.

## Por que existem duas queries de sessão (`_STREAMING` e `_BATCH`)

```python
USER_SESSIONS_SQL_STREAMING = """
SELECT user_id, window_start AS sessao_inicio, window_end AS sessao_fim, ...
FROM TABLE(SESSION(TABLE clickstream PARTITION BY user_id,
                   DESCRIPTOR(event_time), INTERVAL '30' SECOND))
GROUP BY user_id, window_start, window_end
"""

USER_SESSIONS_SQL_BATCH = """
WITH ordered AS (
    SELECT *, LAG(event_time) OVER (PARTITION BY user_id ORDER BY event_time) AS prev_event_time
    FROM clickstream
), ...
"""
```

`USER_SESSIONS_SQL_STREAMING` usa o operador nativo de janela SESSION do
Flink (sintaxe TVF — Table-Valued Function). É a query realmente usada em
produção por `streaming/jobs.py`. `USER_SESSIONS_SQL_BATCH` é uma técnica
SQL diferente — "gap-and-islands": `LAG()` para achar o evento anterior de
cada usuário, marca uma nova sessão sempre que o gap for maior que 30s
(`TIMESTAMPDIFF(SECOND, prev_event_time, event_time) > 30`), depois usa
`SUM() OVER` cumulativo para numerar as sessões e agrupa por
`(user_id, session_num)`. É usada **só** pelos testes bounded
(`tests/test_user_sessions.py`).

A razão de existirem duas: no `apache-flink==1.20.0` pinado neste projeto,
**o operador nativo de janela SESSION — em qualquer uma de suas duas
sintaxes possíveis (a TVF usada aqui, ou a legada `GROUP BY ...
SESSION(...)`) — não compila em modo BATCH**. É uma limitação real do
planejador do Flink nessa versão (`Unaligned windows like session are not
supported in batch mode yet` / `BatchPhysicalWindowAggregateRule` rejeita
`SessionGroupWindow`), não um bug deste projeto — confirmado testando as
duas sintaxes diretamente contra o motor real. Como os testes bounded
rodam em modo batch (`create_batch_env`, decisão deliberada para serem
rápidos e não dependerem de Kafka), a query nativa de sessão nunca
poderia ser exercitada nos testes se fosse a única versão existente — por
isso a técnica gap-and-islands foi desenvolvida como uma segunda
implementação da mesma regra de negócio (fecha sessão após 30s de
inatividade), verificada contra os mesmos casos de teste na divisão de
sessão por gap de inatividade e no isolamento entre usuários diferentes.
**Atenção**: as duas queries agrupam eventos em sessões da mesma forma,
mas não emitem o mesmo `sessao_fim` — a versão streaming usa
`window_end` do Flink (último evento + o gap de 30s), enquanto a versão
batch usa `MAX(event_time)` (o timestamp do último evento, sem o gap).
Nenhum teste compara os dois valores diretamente (os testes de sessão só
verificam contagem de linhas e conjunto de `user_id`), então trate as duas
queries como equivalentes na *lógica de agrupamento*, não como
bit-a-bit idênticas.

Isso poderia sugerir que a produção usa uma lógica "não testada" — não é
bem assim. A sintaxe nativa TVF foi verificada compilando com sucesso em
modo **streaming** via `t_env.explain_sql(...)` contra uma tabela
`datagen` (sem dados reais, só validação do planejador). Depois, na
verificação end-to-end contra Kafka e Postgres reais, ela produziu 45
sessões de usuário corretas — incluindo casos como `USR-0024`, que
acumulou 7 ações e 1 compra de R$399,90 numa única sessão contínua,
respeitando o gap de 30s de inatividade. Ou seja: a lógica de negócio
(o que conta como uma sessão) foi validada pelos testes bounded via
gap-and-islands; a sintaxe nativa usada em produção foi validada
separadamente, primeiro pelo planejador (compila) e depois por dados reais
(produz o resultado esperado) — duas verificações complementares, não uma
lacuna de cobertura.

## Por que o sink é Postgres via JDBC, não tópicos Kafka

O exercício que deu origem a este projeto (`pratica_apache_flink/`, fora
deste repositório) tinha um desenho diferente: os resultados dos jobs Flink
eram publicados de volta em tópicos Kafka dedicados (`metricas-realtime`,
`user-sessions`, e um terceiro para alertas de carrinho abandonado,
`alertas-abandono`). Esse padrão é comum quando o consumidor seguinte
também é outro sistema de streaming, mas cria fricção para dois casos que
importavam aqui:

1. **Consultar o resultado.** Um tópico Kafka não tem uma forma nativa de
   "me mostra o estado atual" — seria necessário outro consumidor (ou o
   Kafka UI) só para inspecionar o que os jobs produziram. Uma tabela
   Postgres responde a `SELECT * FROM sessoes_usuario ORDER BY ...` direto.
2. **Testar o resultado.** Os testes deste projeto (`tests/test_sink_ddl.py`,
   a fixture `clean_db`) conseguem fazer asserções diretas com SQL comum
   contra um banco relacional real, sem precisar de um consumidor Kafka
   dedicado só para ler de volta o que foi escrito.

A migração trocou o conector Kafka de sink pelo conector JDBC
(`streaming/jobs.py`, `METRICS_SINK_DDL`/`SESSIONS_SINK_DDL`, `'connector'
= 'jdbc'`), gravando em tabelas Postgres com schema fixo
(`streaming/sink_ddl.py`) em vez de mensagens JSON num tópico sem schema
imposto. O trade-off é discutido em `docs/conceitos_streaming.md`: sem
two-phase commit, o sink JDBC é at-least-once, não exactly-once.

## O named volume `flink-libs` no `docker-compose.yml`

```yaml
flink-job:
  volumes:
    - .:/app
    - flink-libs:/app/lib
```

O `Dockerfile` baixa os 3 JARs de conector Flink para `/app/lib` durante o
build da imagem. O bind mount `.:/app` — necessário para rodar o código do
host sem rebuildar a imagem a cada mudança — sobrescreve *todo* `/app` da
imagem pelo checkout do host, que não tem esse diretório `lib/` (ele só
existe dentro da imagem construída, não no repositório). Sem o named
volume, isso apagaria os JARs em runtime e o Flink deixaria de enxergar o
conector `kafka` (só os conectores embutidos — `blackhole`, `datagen`,
`filesystem`, `print` — continuariam disponíveis). O named volume
`flink-libs` fica "por cima" do bind mount só no ponto `/app/lib`: o Docker
o popula automaticamente com o conteúdo já existente da imagem nesse
caminho na primeira execução, preservando os JARs mesmo com o bind mount
de código ativo por cima do resto de `/app`.
