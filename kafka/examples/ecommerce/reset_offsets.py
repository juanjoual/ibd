"""
Resetear Offsets de un Grupo de Consumidores
=============================================
Resetea los offsets de un grupo de consumidores al inicio (o a un timestamp
específico) para poder reproducir todos los mensajes desde cero.

IMPORTANTE: Todos los consumidores del grupo deben estar DETENIDOS antes de resetear.

Uso:
    # Resetear al inicio
    python reset_offsets.py --group analytics

    # Resetear a un timestamp específico (formato ISO, UTC)
    python reset_offsets.py --group analytics --timestamp "2026-03-09T10:00:00"

    # Simulación — mostrar qué cambiaría sin resetear realmente
    python reset_offsets.py --group analytics --dry-run
"""

import argparse
import sys
from datetime import datetime, timezone

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient


def get_committed_offsets(bootstrap: str, group: str, topic: str):
    """Obtener los offsets confirmados actuales del grupo en el topic."""
    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": group,
        "enable.auto.commit": False,
    })
    # Necesitamos saber qué particiones existen
    metadata = consumer.list_topics(topic, timeout=10)
    partitions = [
        TopicPartition(topic, p)
        for p in metadata.topics[topic].partitions
    ]
    committed = consumer.committed(partitions, timeout=10)
    consumer.close()
    return committed


def reset_to_earliest(bootstrap: str, group: str, topic: str, dry_run: bool):
    """Resetear todas las particiones al offset 0 (inicio)."""
    committed = get_committed_offsets(bootstrap, group, topic)

    print(f"\n📋 Offsets actuales del grupo '{group}' en el topic '{topic}':")
    for tp in committed:
        print(f"   Partición {tp.partition}: offset {tp.offset}")

    if dry_run:
        print("\n🔍 SIMULACIÓN — se resetearían todas las particiones al offset 0")
        return

    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": group,
        "enable.auto.commit": False,
    })

    # Construir nuevos offsets en la posición 0
    new_offsets = [TopicPartition(topic, tp.partition, 0) for tp in committed]
    consumer.assign(new_offsets)

    # Confirmar los nuevos offsets (reseteados)
    consumer.commit(offsets=new_offsets, asynchronous=False)
    consumer.close()

    print(f"\n✅ Todas las particiones reseteadas al offset 0 para el grupo '{group}'")
    print("   Reinicia tus consumidores para reproducir desde el inicio.\n")


def reset_to_timestamp(bootstrap: str, group: str, topic: str,
                       timestamp_str: str, dry_run: bool):
    """Resetear offsets al primer mensaje en o después de un timestamp dado."""
    ts = datetime.fromisoformat(timestamp_str).replace(tzinfo=timezone.utc)
    ts_ms = int(ts.timestamp() * 1000)

    committed = get_committed_offsets(bootstrap, group, topic)

    # Construir lista de particiones con el timestamp objetivo
    tps = [TopicPartition(topic, tp.partition, ts_ms) for tp in committed]

    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": group,
        "enable.auto.commit": False,
    })

    # Consultar a Kafka el offset correspondiente a cada partición en ese timestamp
    result = consumer.offsets_for_times(tps, timeout=10)

    print(f"\n📋 Offsets para el timestamp {timestamp_str} ({ts_ms} ms):")
    for tp in result:
        print(f"   Partición {tp.partition}: offset {tp.offset}")

    if dry_run:
        print("\n🔍 SIMULACIÓN — se confirmarían los offsets anteriores")
        consumer.close()
        return

    consumer.commit(offsets=result, asynchronous=False)
    consumer.close()

    print(f"\n✅ Offsets reseteados al timestamp {timestamp_str} para el grupo '{group}'")
    print("   Reinicia tus consumidores para reproducir desde ese punto.\n")


def main():
    parser = argparse.ArgumentParser(description="Resetear Offsets de un Grupo de Consumidores")
    parser.add_argument("--bootstrap", default="localhost:9092",
                        help="Servidores bootstrap de Kafka")
    parser.add_argument("--topic", default="orders",
                        help="Topic de Kafka (por defecto: orders)")
    parser.add_argument("--group", default="processing",
                        help="ID del grupo de consumidores a resetear (por defecto: processing)")
    parser.add_argument("--timestamp", default=None,
                        help="Resetear a un timestamp (formato ISO, UTC). Omitir para el inicio.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Mostrar qué cambiaría sin resetear realmente")
    args = parser.parse_args()

    print(f"🔧 Script de reseteo de offsets")
    print(f"   Broker:  {args.bootstrap}")
    print(f"   Topic:   {args.topic}")
    print(f"   Grupo:   {args.group}")

    if args.timestamp:
        reset_to_timestamp(args.bootstrap, args.group, args.topic,
                           args.timestamp, args.dry_run)
    else:
        reset_to_earliest(args.bootstrap, args.group, args.topic, args.dry_run)


if __name__ == "__main__":
    main()
