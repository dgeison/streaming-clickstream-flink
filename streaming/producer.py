import json
import random
import time
from datetime import datetime

from confluent_kafka import Producer

from streaming.events import USUARIOS, generate_event, new_user_state

TOPICO_CLICKSTREAM = "clickstream"


def publicar_clickstream(bootstrap_servers, duracao_segundos, seed=None, sleep_fn=time.sleep):
    rng = random.Random(seed)
    producer = Producer({
        "bootstrap.servers": bootstrap_servers,
        "acks": "all",
        "linger.ms": 10
    })
    estados = {}
    inicio = time.time()
    eventos_enviados = 0

    while time.time() - inicio < duracao_segundos:
        user_id = rng.choice(USUARIOS)
        if user_id not in estados:
            estados[user_id] = new_user_state(rng)
        evento = generate_event(user_id, estados[user_id], rng, datetime.now())
        evento_json = json.dumps(evento, ensure_ascii=False)
        producer.produce(TOPICO_CLICKSTREAM, key=user_id, value=evento_json)
        producer.poll(0)
        eventos_enviados += 1
        sleep_fn(rng.uniform(0.1, 0.4))

    producer.flush(10)
    return eventos_enviados
