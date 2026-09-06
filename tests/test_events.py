import random
from datetime import datetime

from streaming.events import new_user_state, atualizar_carrinho, generate_event


def test_atualizar_carrinho_purchase_clears_cart():
    estado = {"carrinho": [{"id": "PROD-001"}]}
    atualizar_carrinho(estado, "purchase", {"id": "PROD-002"})
    assert estado["carrinho"] == []


def test_atualizar_carrinho_add_to_cart_appends():
    estado = {"carrinho": []}
    produto = {"id": "PROD-001"}
    atualizar_carrinho(estado, "add_to_cart", produto)
    assert estado["carrinho"] == [produto]


def test_atualizar_carrinho_remove_cart_pops_last():
    estado = {"carrinho": [{"id": "PROD-001"}, {"id": "PROD-002"}]}
    atualizar_carrinho(estado, "remove_cart", {"id": "PROD-003"})
    assert estado["carrinho"] == [{"id": "PROD-001"}]


def test_atualizar_carrinho_remove_cart_on_empty_cart_is_noop():
    estado = {"carrinho": []}
    atualizar_carrinho(estado, "remove_cart", {"id": "PROD-001"})
    assert estado["carrinho"] == []


def test_generate_event_is_deterministic_with_same_seed():
    rng1 = random.Random(42)
    estado1 = new_user_state(rng1)
    evento1 = generate_event("USR-0001", estado1, rng1, datetime(2026, 1, 1, 10, 0, 0))

    rng2 = random.Random(42)
    estado2 = new_user_state(rng2)
    evento2 = generate_event("USR-0001", estado2, rng2, datetime(2026, 1, 1, 10, 0, 0))

    assert evento1 == evento2


def test_generate_event_has_expected_fields():
    rng = random.Random(1)
    estado = new_user_state(rng)
    evento = generate_event("USR-0001", estado, rng, datetime(2026, 1, 1, 10, 0, 0))

    expected_fields = {
        "user_id", "session_id", "action", "page", "produto_id", "categoria",
        "valor", "dispositivo", "regiao", "itens_carrinho", "timestamp",
    }
    assert set(evento.keys()) == expected_fields
    assert evento["timestamp"] == "2026-01-01T10:00:00"
    assert evento["user_id"] == "USR-0001"


def test_generate_event_reuses_session_id_across_calls():
    rng = random.Random(5)
    estado = new_user_state(rng)
    evento1 = generate_event("USR-0001", estado, rng, datetime(2026, 1, 1, 10, 0, 0))
    evento2 = generate_event("USR-0001", estado, rng, datetime(2026, 1, 1, 10, 0, 5))
    assert evento1["session_id"] == evento2["session_id"]
