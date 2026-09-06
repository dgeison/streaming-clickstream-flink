# Clickstream em Tempo Real com Kafka + Flink + Postgres

![CI](https://github.com/dgeison/streaming-clickstream-flink/actions/workflows/ci.yml/badge.svg)

Pipeline de streaming de ponta a ponta: um producer gera eventos sintéticos
de clickstream de e-commerce (`page_view`, `click`, `add_to_cart`,
`purchase`, `remove_cart`, `search`) e publica no **Kafka**; um job
**PyFlink** (Table API/SQL) consome esse tópico em modo streaming e calcula
duas visões agregadas em paralelo — métricas por janela de tempo fixa
(TUMBLE) e sessões de usuário por inatividade (SESSION) — gravando os
resultados em **Postgres** via sink JDBC.

## Por que este projeto existe

A maior parte dos tutoriais de streaming para em "leia do Kafka e imprima no
console". Este projeto mostra o próximo passo: duas famílias de janela real
(uma por relógio, outra por comportamento do usuário) rodando como um único
job, com watermark para tolerância a atraso, e um sink que grava em um banco
que dá pra consultar com SQL de verdade depois — não outro tópico Kafka. Ver
`docs/conceitos_streaming.md` para os fundamentos de streaming (event time,
watermark, tipos de janela) e `docs/arquitetura.md` para o detalhe de cada
módulo do código.

## Arquitetura

```
Producer (Python)              Flink Table API (streaming)                 Postgres
     │                                    │                                    │
     ▼                                    ▼                                    ▼
 tópico Kafka          ┌── TUMBLE 1 min  ──────────► metricas_por_janela ─────┤
 `clickstream`  ──────►│                                                      │  (sink JDBC)
                        └── SESSION 30s gap ────────► sessoes_usuario ────────┘
```

As duas queries rodam como um único job Flink via `StatementSet`
(`streaming/jobs.py`), não como dois processos separados.

## Como rodar

Requer só Docker. A sequência abaixo é a que foi de fato usada para validar
o pipeline ponta a ponta contra Kafka e Postgres reais — **a ordem importa**,
em particular o passo de criar o tópico antes de subir o `flink-job` (ver
nota logo abaixo).

```bash
# 1. Sobe a infraestrutura e espera ficar saudável
docker compose up -d kafka postgres
docker compose ps   # confirme kafka e postgres como "healthy"

# 2. Cria o tópico Kafka ANTES de subir o flink-job (ver nota abaixo)
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --create \
  --topic clickstream --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1 --if-not-exists

# 3. Sobe o job Flink em background e confirma que não crashou
docker compose run --rm --name flink-job-e2e flink-job &
sleep 20

# 4. Roda o producer por 90 segundos
docker compose run --rm producer --duracao-segundos 90
# saída esperada: "Producer finalizado: 354 eventos enviados" (número varia)

# 5. Espera a janela TUMBLE de 1 min fechar + gap de sessão de 30s + watermark
sleep 90

# 6. Consulta os resultados
docker compose exec postgres psql -U stream -d streaming \
  -c "SELECT * FROM metricas_por_janela ORDER BY janela_inicio;"
docker compose exec postgres psql -U stream -d streaming \
  -c "SELECT * FROM sessoes_usuario ORDER BY sessao_inicio LIMIT 10;"

# 7. Limpeza
docker stop flink-job-e2e
docker compose down
```

**Por que criar o tópico manualmente (passo 2) é obrigatório:** o Kafka do
compose já roda com `KAFKA_AUTO_CREATE_TOPICS_ENABLE=true`, mas isso só
cria tópicos automaticamente em chamadas de *produce/consume*. A descoberta
de partições que o Flink faz ao registrar a tabela `clickstream`
(`AdminClient.describeTopics`, dentro do enumerador de source do conector
Kafka) **não** aciona essa auto-criação. Como o `flink-job` sobe antes do
`producer` de propósito (para não perder eventos, já que a source usa
`scan.startup.mode=latest-offset`), o tópico genuinamente não existe ainda
nesse ponto — sem o passo 2, o `flink-job` falha de forma determinística
com `UnknownTopicOrPartitionException` toda vez que o Kafka está vazio
(primeira execução, ou depois de qualquer `docker compose down -v`).

**Nota sobre o volume `flink-libs`:** o serviço `flink-job` monta tanto
`.:/app` (código-fonte ativo, útil para editar sem rebuildar a imagem)
quanto um named volume `flink-libs:/app/lib`. O segundo existe porque o
bind mount do código sobrescreveria `/app` inteiro, apagando os 3 JARs de
conector Flink (Kafka, JDBC, driver Postgres) que o `Dockerfile` baixa em
`/app/lib` durante o build — o named volume "protege" só esse subdiretório,
preservando os JARs mesmo com o bind mount por cima. Detalhe completo em
`docs/arquitetura.md`.

### Exemplo de saída real

Rodando a sequência acima (producer por 90s, ~354 eventos publicados):

```
$ psql ... -c "SELECT * FROM metricas_por_janela ORDER BY janela_inicio;"
    janela_inicio    |     janela_fim      | total_eventos | usuarios_ativos | page_views | clicks | add_to_carts | purchases | receita
---------------------+---------------------+---------------+-----------------+------------+--------+--------------+-----------+---------
 2026-09-06 13:05:00 | 2026-09-06 13:06:00 |           109 |              58 |         21 |     33 |           13 |         3 |   639.7
(1 row)
```

Só 1 linha porque a janela TUMBLE é de 1 minuto e o producer rodou 90s — a
segunda janela ainda estava aberta quando a consulta rodou (esperado).

```
$ psql ... -c "SELECT * FROM sessoes_usuario ORDER BY sessao_inicio LIMIT 10;"
 user_id  |    sessao_inicio    |     sessao_fim      | acoes_total | compras | receita_sessao
----------+---------------------+---------------------+-------------+---------+----------------
 USR-0013 | 2026-09-06 13:05:33 | 2026-09-06 13:06:03 |           1 |       0 |              0
 USR-0075 | 2026-09-06 13:05:33 | 2026-09-06 13:06:26 |           2 |       0 |              0
 USR-0011 | 2026-09-06 13:05:33 | 2026-09-06 13:06:03 |           1 |       0 |              0
 USR-0025 | 2026-09-06 13:05:34 | 2026-09-06 13:06:11 |           3 |       0 |              0
 USR-0002 | 2026-09-06 13:05:34 | 2026-09-06 13:06:04 |           1 |       0 |              0
 USR-0044 | 2026-09-06 13:05:34 | 2026-09-06 13:06:42 |           4 |       0 |              0
 USR-0063 | 2026-09-06 13:05:35 | 2026-09-06 13:06:05 |           1 |       0 |              0
 USR-0003 | 2026-09-06 13:05:35 | 2026-09-06 13:06:40 |           4 |       0 |              0
 USR-0059 | 2026-09-06 13:05:35 | 2026-09-06 13:06:44 |           4 |       0 |              0
 USR-0070 | 2026-09-06 13:05:36 | 2026-09-06 13:06:14 |           2 |       0 |              0
(10 rows, de 45 no total)
```

A tabela completa (45 sessões) teve casos como `USR-0024`, que acumulou 7
ações e 1 compra de R$399,90 numa janela de sessão de ~64s — o gap de
inatividade de 30s foi respeitado corretamente contra dados reais do
Kafka. Importante: `sessao_fim` é o timestamp do **último evento + o gap
de 30s** (é assim que o Flink define o fim de uma janela SESSION), não o
timestamp do último evento em si — então dos ~64s, cerca de 30s são o gap
de "espera para ver se o usuário volta", não atividade real. Isso fica
óbvio nas sessões de 1 ação da tabela acima (ex.: `USR-0013`,
13:05:33→13:06:03): são exatamente 30s de duração, ou seja, 100% gap, 0%
atividade adicional.

## Testes

```bash
docker compose run --rm test -v
```

Suíte roda contra o motor Flink real em modo **batch** (fonte bounded via
CSV, sem precisar de Kafka) e contra Postgres real via `docker-compose`
network — nada é mockado. Última execução: `15 passed, 0 failed`.

## O que fica de fora (por enquanto)

- **Job de alerta de carrinho abandonado**: o exercício que serviu de
  semente para este projeto (`pratica_apache_flink/07_kafka_to_kafka.py`)
  detectava carrinhos abandonados e publicava alertas num tópico Kafka
  dedicado (`alertas-abandono`). Não foi trazido para cá — teria a mesma
  forma dos dois jobs existentes (uma terceira query no `StatementSet`),
  mas ficou fora do escopo deste flagship.
- **Sink Elasticsearch**: os dois sinks atuais são tabelas Postgres via
  JDBC. Um sink adicional em Elasticsearch (para full-text search ou um
  dashboard Kibana sobre o clickstream bruto) é uma extensão natural, não
  implementada.
- **Interface Flink SQL Client**: toda interação com o Flink aqui é
  programática, via Table API em Python (`streaming/jobs.py`). Não há uma
  sessão interativa via SQL Client exposta para explorar as tabelas
  manualmente.
- **Cluster Flink distribuído**: o PyFlink aqui roda em modo de execução
  local (um mini-cluster embutido no próprio processo Python), não um
  cluster real com JobManager/TaskManager separados. Suficiente para
  demonstrar a lógica de streaming sem a complexidade operacional de um
  cluster distribuído de verdade.
- **Teste de integração end-to-end contra Kafka real**: a suíte de testes
  roda as mesmas queries de agregação contra uma fonte bounded (CSV, modo
  batch) em vez de produzir/consumir de um Kafka real — decisão
  deliberada para manter os testes rápidos e determinísticos (ver seção
  "Testes" acima e `docs/conceitos_streaming.md`). A prova de que o
  pipeline funciona de ponta a ponta contra Kafka real está na seção
  "Exemplo de saída real" acima, mas não é uma automatizada.

(Mesmo tratamento dado à variante AWS/cloud citada nos outros dois
flagships deste portfólio: evolução planejada, não implementada.)
