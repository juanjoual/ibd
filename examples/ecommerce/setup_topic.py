"""
Configuración de Topic
=======================
Crea el topic 'orders' con 6 particiones (una por categoría).
Ejecutar una vez antes de iniciar la demo.

Uso:
    python setup_topic.py
    python setup_topic.py --topic orders --partitions 6
    python setup_topic.py --delete   # eliminar y recrear
"""

import argparse
import time

from confluent_kafka.admin import AdminClient, NewTopic


def main():
    parser = argparse.ArgumentParser(description="Crear el topic de pedidos")
    parser.add_argument("--bootstrap", default="localhost:9092",
                        help="Servidores bootstrap de Kafka")
    parser.add_argument("--topic", default="orders",
                        help="Nombre del topic (por defecto: orders)")
    parser.add_argument("--partitions", type=int, default=1,
                        help="Número de particiones (por defecto: 1)")
    parser.add_argument("--delete", action="store_true",
                        help="Eliminar el topic primero si existe")
    args = parser.parse_args()

    admin = AdminClient({"bootstrap.servers": args.bootstrap})

    # Comprobar topics existentes
    metadata = admin.list_topics(timeout=10)
    topic_exists = args.topic in metadata.topics

    # Eliminar si se solicita
    if args.delete and topic_exists:
        print(f"🗑️  Eliminando topic existente '{args.topic}'...")
        futures = admin.delete_topics([args.topic])
        for topic, future in futures.items():
            try:
                future.result()
                print(f"   ✅ Topic '{topic}' eliminado")
            except Exception as e:
                print(f"   ❌ Error al eliminar '{topic}': {e}")
        # Esperar a que se propague la eliminación
        time.sleep(2)
        topic_exists = False

    if topic_exists:
        existing_partitions = len(metadata.topics[args.topic].partitions)
        print(f"ℹ️  El topic '{args.topic}' ya existe con {existing_partitions} partición(es)")
        if existing_partitions != args.partitions:
            print(f"   ⚠️  Solicitaste {args.partitions} particiones. "
                  f"Usa --delete para recrear con el número correcto.")
        return

    # Crear el topic
    print(f"📝 Creando topic '{args.topic}' con {args.partitions} particiones...")
    new_topic = NewTopic(
        args.topic,
        num_partitions=args.partitions,
        replication_factor=1,
    )

    futures = admin.create_topics([new_topic])
    for topic, future in futures.items():
        try:
            future.result()
            print(f"   ✅ Topic '{topic}' creado correctamente!")
        except Exception as e:
            print(f"   ❌ Error al crear '{topic}': {e}")

    # Verificar
    metadata = admin.list_topics(timeout=10)
    if args.topic in metadata.topics:
        n = len(metadata.topics[args.topic].partitions)
        print(f"\n🔍 Verificación: El topic '{args.topic}' tiene {n} partición(es)")
        for pid in sorted(metadata.topics[args.topic].partitions.keys()):
            print(f"   Partición {pid}: ✓")


if __name__ == "__main__":
    main()
