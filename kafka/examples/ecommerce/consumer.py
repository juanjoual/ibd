"""
Consumidor de Pedidos E-Commerce
================================
Consumidor flexible que se une a un grupo de consumidores y procesa pedidos.
Ejecuta múltiples instancias con el mismo --group para ver el rebalanceo de particiones.

Uso:
    # Grupo 1 — "procesamiento de pedidos" (valida pedidos)
    python consumer.py --group processing
    python consumer.py --group processing   # ¡escalar!
    python consumer.py --group processing   # ¡escalar más!

    # Grupo 2 — "analítica" (calcula ingresos por categoría)
    python consumer.py --group analytics

    # Grupo 3 — "notificaciones" (imprime confirmaciones al cliente)
    python consumer.py --group notifications

    # Reproducir desde el inicio (ignora offsets confirmados)
    python consumer.py --group analytics --replay

    # Commit manual (at-least-once) en vez de auto-commit (at-most-once)
    python consumer.py --group processing --manual-commit
"""

import argparse
import json
import random
import signal
import sys
from collections import defaultdict
from datetime import datetime

from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition

# ─── Colores para salida en terminal ──────────────────────────────────────────
COLORS = ["\033[96m", "\033[93m", "\033[92m", "\033[95m", "\033[91m", "\033[94m"]
RESET = "\033[0m"
BOLD = "\033[1m"
DIM = "\033[2m"


def color_for_partition(partition: int) -> str:
    return COLORS[partition % len(COLORS)]


# ─── Callback de rebalanceo ──────────────────────────────────────────────────
class RebalanceLogger:
    """Registra eventos de asignación / revocación de particiones."""

    def __init__(self, consumer: Consumer, consumer_id: str, replay: bool):
        self.consumer = consumer
        self.consumer_id = consumer_id
        self.replay = replay

    def __call__(self, consumer, partitions):
        # confluent_kafka dispara esto tanto para asignar como para revocar
        if partitions and partitions[0].offset == -1001:  # OFFSET_INVALID → asignación
            self._on_assign(consumer, partitions)
        else:
            self._on_revoke(consumer, partitions)

    def _on_assign(self, consumer, partitions):
        parts = ", ".join(str(p.partition) for p in partitions)
        print(f"\n{BOLD}🔄 REBALANCEO — {self.consumer_id} PARTICIONES ASIGNADAS: [{parts}]{RESET}")

        if self.replay:
            # Posicionar cada partición asignada al inicio
            for p in partitions:
                p.offset = 0  # OFFSET_BEGINNING
            consumer.assign(partitions)
            print(f"   ⏪ Modo reproducción: posicionando todas las particiones asignadas al offset 0")
        
        print()

    def _on_revoke(self, consumer, partitions):
        parts = ", ".join(str(p.partition) for p in partitions)
        print(f"\n{BOLD}🔄 REBALANCEO — {self.consumer_id} PARTICIONES REVOCADAS: [{parts}]{RESET}\n")


# ─── Lógica de procesamiento por rol de grupo ───────────────────────────────────
class OrderProcessor:
    """Simula diferente lógica de procesamiento según el grupo de consumidores."""

    def __init__(self, group: str, consumer_id: str):
        self.group = group
        self.consumer_id = consumer_id
        self.count = 0
        self.revenue = defaultdict(float)  # categoría → ingresos totales

    def process(self, msg):
        self.count += 1
        order = json.loads(msg.value().decode())
        partition = msg.partition()
        offset = msg.offset()
        pc = color_for_partition(partition)

        # Encabezado común
        header = (
            f"{pc}[P{partition}|O{offset}]{RESET} "
            f"{DIM}{self.consumer_id}{RESET} "
        )

        if "processing" in self.group:
            self._process_order(header, order)
        elif "analytics" in self.group:
            self._process_analytics(header, order)
        elif "notification" in self.group:
            self._process_notification(header, order)
        else:
            self._process_generic(header, order)

    def _process_order(self, header, order):
        status = "✅ VÁLIDO" if order["total"] < 500 else "⚠️  REVISIÓN (pedido grande)"
        print(
            f"{header}📦 Pedido {order['order_id']}: "
            f"{order['quantity']}x {order['product']} = ${order['total']:.2f} → {status}"
        )

    def _process_analytics(self, header, order):
        cat = order["category"]
        self.revenue[cat] += order["total"]
        print(
            f"{header}📊 +${order['total']:.2f} [{cat}] "
            f"| Acumulado {cat}: ${self.revenue[cat]:.2f} "
            f"| Total general: ${sum(self.revenue.values()):.2f}"
        )

    def _process_notification(self, header, order):
        print(
            f"{header}📧 → {order['customer']}: "
            f"¡Tu pedido #{order['order_id']} de {order['product']} ha sido recibido!"
        )

    def _process_generic(self, header, order):
        print(
            f"{header}📨 {order['order_id']} | {order['customer']} | "
            f"{order['category']} | {order['product']} | ${order['total']:.2f}"
        )

    def summary(self):
        print(f"\n{'─' * 60}")
        print(f"📈 {self.consumer_id} Resumen: {self.count} mensajes procesados")
        if self.revenue:
            print("   Ingresos por categoría:")
            for cat, total in sorted(self.revenue.items()):
                print(f"     {cat:15s}  ${total:>10.2f}")
            print(f"     {'─' * 28}")
            print(f"     {'TOTAL':15s}  ${sum(self.revenue.values()):>10.2f}")


# ─── Main ──────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Consumidor de Pedidos E-Commerce")
    parser.add_argument("--bootstrap", default="localhost:9092", help="Servidores bootstrap de Kafka")
    parser.add_argument("--topic", default="orders", help="Topic de Kafka (por defecto: orders)")
    parser.add_argument("--group", default="processing", help="ID del grupo de consumidores")
    parser.add_argument("--replay", action="store_true",
                        help="Reproducir desde el inicio (ignora offsets confirmados)")
    parser.add_argument("--manual-commit", action="store_true",
                        help="Commit manual tras procesar cada mensaje (at-least-once)")
    args = parser.parse_args()

    worker_id = f"{args.group}-{random.randint(1, 99):03d}"

    # ── Garantías de entrega ──────────────────────────────────────────────────
    # enable.auto.commit = True  (por defecto)
    #   Kafka confirma offsets automáticamente cada N ms.
    #   Riesgo: si el consumidor se cae DESPUÉS de confirmar pero ANTES de
    #   terminar de procesar, ese mensaje se pierde → AT-MOST-ONCE.
    #
    # enable.auto.commit = False  (--manual-commit)
    #   Tú decides cuándo confirmar. Se confirma DESPUÉS de procesar.
    #   Riesgo: si se cae después de procesar pero antes de confirmar,
    #   el mensaje se re-entrega → AT-LEAST-ONCE (posibles duplicados,
    #   pero nunca se pierde un mensaje).

    conf = {
        "bootstrap.servers": args.bootstrap,
        "group.id":          args.group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": not args.manual_commit,
        "auto.commit.interval.ms": 3000,
        # Rebalanceo ligeramente más rápido para la demo
        "session.timeout.ms": 10000,
        "max.poll.interval.ms": 30000,
    }

    consumer = Consumer(conf)
    processor = OrderProcessor(args.group, worker_id)
    rebalance_cb = RebalanceLogger(consumer, worker_id, args.replay)

    consumer.subscribe([args.topic], on_assign=rebalance_cb._on_assign,
                       on_revoke=rebalance_cb._on_revoke)

    print(f"🎧 Consumidor '{worker_id}' | Grupo '{args.group}' | Topic '{args.topic}'")
    if args.replay:
        print("   ⏪ MODO REPRODUCCIÓN — leerá desde el offset 0 en cada partición asignada")
    print("   Esperando mensajes (Ctrl+C para detener)\n" + "─" * 60)

    # Apagado graceful con Ctrl+C
    running = True

    def shutdown(sig, frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    try:
        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())
            processor.process(msg)

            # En modo manual, confirmamos el offset DESPUÉS de procesar.
            # Si el consumidor se cae antes de esta línea, el mensaje
            # se re-entregará en el próximo arranque (at-least-once).
            if args.manual_commit:
                consumer.commit(message=msg, asynchronous=False)

    finally:
        processor.summary()
        consumer.close()
        print("👋 Consumidor cerrado correctamente.")


if __name__ == "__main__":
    main()
