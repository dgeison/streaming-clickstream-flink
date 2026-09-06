# Conceitos de Streaming (Kafka + Flink), na Prática

## Event time vs. processing time

**Processing time** é o relógio da máquina que está rodando o Flink no
momento em que ela processa o evento. **Event time** é o instante em que o
evento *aconteceu* de verdade, segundo o próprio evento — nesse projeto, o
campo `timestamp` que o producer grava (`streaming/events.py`,
`generate_event`, formato `"yyyy-MM-ddTHH:mm:ss"`).

O DDL da tabela `clickstream` (`streaming/jobs.py`) declara isso
explicitamente:

```sql
`timestamp` STRING,
event_time AS TO_TIMESTAMP(`timestamp`, 'yyyy-MM-dd''T''HH:mm:ss'),
WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
```

`event_time` é uma coluna computada a partir do campo `timestamp` do
próprio evento — não do relógio do processo Flink (`processing time` seria
o default se essa coluna nem existisse, e o Flink agrupasse por quando o
registro chegou no operador). Isso importa porque o `timestamp` é gerado no
producer, mas o evento pode chegar ao Flink com atraso (fila do Kafka,
rede, reprocessamento) — usar event time garante que um evento das
`10:00:05` sempre caia na janela das `10:00:05`, não importa quando o Flink
efetivamente o processou.

## Watermark: tolerância a atraso

Um watermark é uma marca d'água lógica que o Flink usa para decidir "não
espero mais nenhum evento com event time anterior a este ponto" — é o
mecanismo que permite fechar uma janela (TUMBLE ou SESSION) mesmo em um
stream infinito, sem esperar para sempre por dados atrasados.

O job real usa `event_time - INTERVAL '10' SECOND`: o watermark avança 10
segundos atrás do maior event time já visto, dando 10 segundos de tolerância
para eventos que cheguem fora de ordem antes de considerar uma janela
"fechada" e disparar o resultado. Esse valor é uma escolha prática — grande
o bastante para absorver atraso de rede/Kafka num ambiente local, pequeno o
bastante para não segurar demais o fechamento das janelas.

Os testes bounded (`tests/helpers.py`) usam watermark `0`:

```sql
WATERMARK FOR event_time AS event_time
```

Isso não é uma inconsistência — é a escolha correta para o cenário: os
testes rodam em modo **batch** (`create_batch_env`), contra uma fonte CSV
finita onde **todos os dados já estão disponíveis antes da query rodar**.
Não existe "atraso" a tolerar porque não existe um relógio de parede
avançando enquanto o Flink processa — o conjunto de dados é conhecido de
antemão e já está ordenável. Um watermark de 10s ali só adicionaria uma
espera artificial sem nenhum benefício, então os testes usam o valor mínimo
que ainda satisfaz a exigência sintática do Flink de que toda tabela
event-time tenha uma cláusula `WATERMARK`.

## TUMBLE vs. SESSION: janela fixa vs. janela por inatividade

**TUMBLE** (`METRICS_WINDOW_SQL`, `streaming/queries.py`) é uma janela de
tamanho fixo, alinhada ao relógio, sem sobreposição — todo evento cai em
exatamente uma janela de 1 minuto:

```sql
FROM TABLE(TUMBLE(TABLE clickstream, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))
GROUP BY window_start, window_end
```

Isso responde perguntas do tipo "quantas compras aconteceram entre 13:05 e
13:06?" — útil para métricas agregadas por minuto/hora, independente do
comportamento de qualquer usuário individual.

**SESSION** (`USER_SESSIONS_SQL_STREAMING`, mesmo arquivo) é uma janela
dinâmica por usuário: o tamanho da janela não é fixo, ela se estende
enquanto o usuário continuar ativo e só "fecha" depois de um gap de
inatividade — aqui, 30 segundos sem nenhum evento daquele `user_id`:

```sql
FROM TABLE(SESSION(TABLE clickstream PARTITION BY user_id,
                   DESCRIPTOR(event_time), INTERVAL '30' SECOND))
GROUP BY user_id, window_start, window_end
```

Isso responde perguntas do tipo "quanto tempo o USR-0024 navegou antes de
comprar, numa única visita?" — uma sessão de navegação real, não um recorte
arbitrário de relógio. No E2E real (ver README), `USR-0024` acumulou 7
ações e 1 compra de R$399,90 numa sessão contínua de ~64 segundos — se
fosse janela TUMBLE de 1 minuto, essas ações poderiam ter sido cortadas ao
meio por uma fronteira de relógio arbitrária; com SESSION, a sessão inteira
do usuário fica junta.

## At-least-once no sink JDBC: o que acontece se o job reiniciar no meio de uma janela

O conector JDBC do Flink não implementa two-phase commit (diferente do
sink Kafka, que suporta transações) — então ele não oferece garantia
exactly-once por padrão. O padrão é **at-least-once**: se o job Flink for
reiniciado a partir de um checkpoint, o resultado de uma janela que já
tinha sido escrita no Postgres antes do checkpoint pode ser recalculado e
reescrito depois da recuperação — o mesmo resultado pode ser gravado mais
de uma vez.

O que acontece com essa regravação depende de o sink operar em modo
**append** (`INSERT` puro) ou **upsert** (`INSERT ... ON CONFLICT`) — e
isso é decidido pela presença de uma `PRIMARY KEY` na definição da tabela
*do lado do Flink*. As DDLs de sink em `streaming/jobs.py`
(`METRICS_SINK_DDL`, `SESSIONS_SINK_DDL`) **não declaram** `PRIMARY KEY`,
mesmo as tabelas Postgres reais (`streaming/sink_ddl.py`) tendo uma
(`PRIMARY KEY (janela_inicio, janela_fim)` e
`PRIMARY KEY (user_id, sessao_inicio)`, respectivamente). Sem `PRIMARY KEY`
do lado do Flink, o conector JDBC grava em modo append — uma regravação
depois de um restart não faz upsert silencioso, ela colide com a
constraint de chave primária do Postgres e o `INSERT` falha com erro de
chave duplicada.

Vale registrar também que, como está configurado hoje
(`streaming/flink_env.py`), `create_streaming_env()` não habilita
checkpointing (`execution.checkpointing.interval` não é setado) — então,
na prática, um restart do `flink-job` neste projeto não recupera estado
nenhum: qualquer janela ainda aberta em memória no momento do crash é
perdida (não há o que "reemitir"), e a fonte Kafka reabre com
`scan.startup.mode = 'latest-offset'`, ou seja, o job recomeça do fim do
tópico, sem reprocessar o intervalo perdido. Isso é intencional para manter
o job simples num ambiente de demonstração, mas é o tipo de trade-off que,
em produção, exigiria habilitar checkpointing e decidir explicitamente
entre aceitar duplicatas (declarando `PRIMARY KEY` no sink para virar
upsert) ou aceitar a perda de dados em caso de crash.
