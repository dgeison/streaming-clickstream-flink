USUARIOS = [f"USR-{i:04d}" for i in range(1, 81)]
DISPOSITIVOS = ["desktop", "mobile", "tablet"]
REGIOES = ["Sudeste", "Sul", "Nordeste", "Centro-Oeste", "Norte"]
PAGINAS = ["/home", "/categorias", "/ofertas", "/minha-conta", "/carrinho"]

CATEGORIAS = {
    "eletronicos": [
        {"id": "PROD-001", "nome": "Notebook Dell", "preco": 3499.99},
        {"id": "PROD-002", "nome": "iPhone 15 Pro", "preco": 7999.00},
        {"id": "PROD-003", "nome": "Monitor LG 27", "preco": 1599.00},
        {"id": "PROD-004", "nome": "Teclado Mecanico", "preco": 399.90},
    ],
    "livros": [
        {"id": "PROD-010", "nome": "Data-Intensive Apps", "preco": 189.90},
        {"id": "PROD-011", "nome": "Clean Code", "preco": 129.90},
    ],
    "roupas": [
        {"id": "PROD-020", "nome": "Camiseta Preta", "preco": 49.90},
        {"id": "PROD-021", "nome": "Tenis Nike", "preco": 599.00},
    ],
}

TRANSICOES = {
    "page_view": {
        "page_view": 3,
        "search": 3,
        "click": 3,
        "add_to_cart": 1,
        "purchase": 0,
        "remove_cart": 0,
    },
    "search": {
        "page_view": 1,
        "search": 1,
        "click": 6,
        "add_to_cart": 1,
        "purchase": 0,
        "remove_cart": 1,
    },
    "click": {
        "page_view": 1,
        "search": 1,
        "click": 1,
        "add_to_cart": 5,
        "purchase": 0,
        "remove_cart": 2,
    },
    "add_to_cart": {
        "page_view": 2,
        "search": 1,
        "click": 2,
        "add_to_cart": 1,
        "purchase": 3,
        "remove_cart": 1,
    },
    "purchase": {
        "page_view": 7,
        "search": 2,
        "click": 1,
        "add_to_cart": 0,
        "purchase": 0,
        "remove_cart": 0,
    },
    "remove_cart": {
        "page_view": 3,
        "search": 3,
        "click": 2,
        "add_to_cart": 1,
        "purchase": 1,
        "remove_cart": 0,
    },
}


def new_user_state(rng):
    return {
        "session_id": None,
        "dispositivo": rng.choice(DISPOSITIVOS),
        "regiao": rng.choice(REGIOES),
        "ultima_acao": "page_view",
        "carrinho": [],
    }


def atualizar_carrinho(estado, action, produto):
    if action == "add_to_cart":
        estado["carrinho"].append(produto)
    elif action == "purchase":
        estado["carrinho"] = []
    elif action == "remove_cart" and estado["carrinho"]:
        estado["carrinho"].pop()


def generate_event(user_id, estado, rng, timestamp):
    if estado["session_id"] is None:
        estado["session_id"] = f"SES-{user_id}-{int(timestamp.timestamp())}"

    pesos = TRANSICOES.get(estado["ultima_acao"], TRANSICOES["page_view"])
    action = rng.choices(list(pesos.keys()), weights=list(pesos.values()), k=1)[0]

    categoria = rng.choice(list(CATEGORIAS.keys()))
    produto = rng.choice(CATEGORIAS[categoria])
    valor_actions = ("click", "add_to_cart", "purchase", "remove_cart")
    valor = produto["preco"] if action in valor_actions else 0.0
    page_actions = ("click", "add_to_cart")
    page = (
        f"/produto/{produto['id'].lower()}"
        if action in page_actions
        else rng.choice(PAGINAS)
    )

    atualizar_carrinho(estado, action, produto)
    estado["ultima_acao"] = action

    return {
        "user_id": user_id,
        "session_id": estado["session_id"],
        "action": action,
        "page": page,
        "produto_id": produto["id"],
        "categoria": categoria,
        "valor": valor,
        "dispositivo": estado["dispositivo"],
        "regiao": estado["regiao"],
        "itens_carrinho": len(estado["carrinho"]),
        "timestamp": timestamp.strftime("%Y-%m-%dT%H:%M:%S"),
    }
