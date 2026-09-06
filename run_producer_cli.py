import argparse
import os

from streaming.producer import publicar_clickstream


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--duracao-segundos", type=int, default=120)
    parser.add_argument("--seed", type=int, default=None)
    args = parser.parse_args()

    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    total = publicar_clickstream(bootstrap_servers, args.duracao_segundos, seed=args.seed)
    print(f"Producer finalizado: {total} eventos enviados")


if __name__ == "__main__":
    main()
