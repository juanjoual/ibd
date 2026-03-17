"""
Productor de Pedidos E-Commerce
===============================
Genera pedidos aleatorios y los envía a un topic de Kafka particionado por categoría.

Uso:
    python producer.py                      # por defecto: 1 pedido/segundo
    python producer.py --rate 0.2           # 1 pedido cada 5 segundos
    python producer.py --rate 5             # 5 pedidos por segundo (modo ráfaga)
    python producer.py --acks all           # esperar confirmación de todas las réplicas
    python producer.py --acks 0             # fire-and-forget (sin confirmación)
    python producer.py --bootstrap localhost:9092
"""

import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer

# ─── Catálogo de productos ──────────────────────────────────────────────────
CATALOG = {
    "electrónica": [
        {"product": "Auriculares Inalámbricos", "price": 79.99},
        {"product": "Hub USB-C",                "price": 34.99},
        {"product": "Teclado Mecánico",         "price": 129.99},
        {"product": "Monitor 4K",               "price": 399.99},
        {"product": "Webcam HD",                "price": 59.99},
    ],
    "ropa": [
        {"product": "Zapatillas de Running", "price": 89.99},
        {"product": "Chaqueta de Invierno",  "price": 149.99},
        {"product": "Camiseta de Algodón",   "price": 19.99},
        {"product": "Vaqueros",              "price": 49.99},
        {"product": "Bufanda de Lana",       "price": 29.99},
    ],
    "alimentación": [
        {"product": "Café Orgánico 1kg",       "price": 14.99},
        {"product": "Caja de Chocolate Negro", "price": 9.99},
        {"product": "Aceite de Oliva 500ml",   "price": 12.49},
        {"product": "Barritas Proteicas (12)", "price": 24.99},
        {"product": "Té Verde (100 bolsas)",   "price": 7.99},
    ],
    "libros": [
        {"product": "Designing Data-Intensive Applications", "price": 39.99},
        {"product": "Kafka: The Definitive Guide",          "price": 44.99},
        {"product": "Python Crash Course",                   "price": 29.99},
        {"product": "Clean Code",                            "price": 34.99},
        {"product": "The Pragmatic Programmer",              "price": 42.99},
    ],
    "deportes": [
        {"product": "Esterilla de Yoga",    "price": 24.99},
        {"product": "Bandas Elásticas",     "price": 14.99},
        {"product": "Comba",               "price": 9.99},
        {"product": "Rodillo de Espuma",    "price": 19.99},
        {"product": "Botella de Agua 1L",   "price": 12.99},
    ],
    "hogar": [
        {"product": "Set de Velas Aromáticas", "price": 22.99},
        {"product": "Manta de Sofá",           "price": 34.99},
        {"product": "Lámpara LED de Escritorio","price": 27.99},
        {"product": "Juego de Tazas (4)",      "price": 18.99},
        {"product": "Reloj de Pared",          "price": 15.99},
    ],
}

CATEGORIES = list(CATALOG.keys())

CUSTOMERS = [
    "Alicia", "Andrés", "Beatriz", "Carlos", "Carmen",
    "Diego", "Elena", "Fernando", "Gloria", "Hugo",
    "Isabel", "Javier", "Manuel", "Lucía", "Juanjo"
]


def generate_order() -> dict:
    """Construye un pedido aleatorio."""
    category = random.choice(CATEGORIES)
    item = random.choice(CATALOG[category])
    quantity = random.randint(1, 5)

    return {
        "order_id":  str(uuid.uuid4())[:8],
        "customer":  random.choice(CUSTOMERS),
        "product":   item["product"],
        "category":  category,
        "quantity":   quantity,
        "unit_price": item["price"],
        "total":      round(item["price"] * quantity, 2),
        "timestamp":  datetime.now(timezone.utc).isoformat(),
    }


def delivery_report(err, msg):
    if err:
        print(f"  ❌ Entrega fallida: {err}")
    else:
        print(
            f"  ✅ Partición {msg.partition()} | "
            f"Offset {msg.offset()} | "
            f"Clave: {msg.key().decode()}"
        )


def main():
    parser = argparse.ArgumentParser(description="Productor de Pedidos E-Commerce")
    parser.add_argument("--bootstrap", default="localhost:9092", help="Servidores bootstrap de Kafka")
    parser.add_argument("--topic", default="orders", help="Topic de Kafka (por defecto: orders)")
    parser.add_argument("--rate", type=float, default=1.0, help="Pedidos por segundo (por defecto: 1)")
    parser.add_argument("--acks", default="all", choices=["0", "1", "all"],
                        help="Nivel de confirmación: 0=fire-and-forget, 1=solo líder, all=todas las réplicas (por defecto: all)")
    args = parser.parse_args()

    # ── Configuración del productor ─────────────────────────────────────
    # acks:  cuántas réplicas deben confirmar antes de considerar el mensaje entregado
    #   "0"   → fire-and-forget (más rápido, puede perder datos)
    #   "1"   → solo el líder confirma (riesgo si el líder muere antes de replicar)
    #   "all" → líder + todas las réplicas in-sync confirman (más seguro)
    #
    # enable.idempotence: evita duplicados cuando el productor reintenta un envío
    #   El broker asigna un Producer ID + número de secuencia a cada mensaje;
    #   si recibe el mismo (PID, seq) dos veces, lo descarta silenciosamente.
    #   Requiere acks=all (se activa automáticamente).
    use_idempotence = args.acks == "all"

    producer = Producer({
        "bootstrap.servers":  args.bootstrap,
        "client.id":          "order-producer",
        "acks":               args.acks,
        "enable.idempotence": use_idempotence,
    })

    delay = 1.0 / args.rate
    count = 0

    idem_label = "SÍ" if use_idempotence else "NO"
    print(f"🛒  Productor de Pedidos → topic '{args.topic}' @ {args.rate} pedidos/seg")
    print(f"    acks={args.acks} | idempotente={idem_label}")
    print(f"    Categorías (claves): {', '.join(CATEGORIES)}")
    print("    Pulsa Ctrl+C para detener\n" + "─" * 60)

    try:
        while True:
            order = generate_order()
            count += 1

            print(f"\n📦 Pedido #{count}: {order['customer']} → "
                  f"{order['quantity']}x {order['product']} "
                  f"(${order['total']:.2f}) [{order['category']}]")

            producer.produce(
                topic=args.topic,
                key=order["category"].encode(),   # particionar por categoría
                value=json.dumps(order).encode(),
                callback=delivery_report,
            )
            producer.poll(0)

            time.sleep(delay)

    except KeyboardInterrupt:
        print(f"\n🛑 Deteniendo después de {count} pedidos...")
    finally:
        remaining = producer.flush(timeout=5)
        if remaining:
            print(f"⚠️  {remaining} mensaje(s) no fueron entregados")
        else:
            print("✅ Todos los mensajes entregados. ¡Listo!")


if __name__ == "__main__":
    main()
