# Pipeline de Pedidos E-Commerce — Demo de Kafka

Demo práctica que utiliza un flujo simulado de pedidos e-commerce para ilustrar **particiones**, **grupos de consumidores**, **escalado horizontal** y **reproducción de mensajes**.


## Escenarios de demostración


### Demo 1 Particiones

Iniciar productor + un consumidor. Observar que la **clave de categoría (key)** determina a qué partición llega cada mensaje. Misma clave → misma partición, siempre.

```bash
# Creamos el topic "orders" con 6 particiones
python setup_topic.py --delete --partitions=6

# Terminal 1
python producer.py

# Terminal 2
python consumer.py --group processing
```

Fíjate en el prefijo `[P3|O42]` en cada mensaje — los pedidos con la misma categoría siempre aparecen en el mismo número de partición.

---

### Demo 2: Grupos de consumidores

Iniciar tres grupos de consumidores simultáneamente. Cada grupo lee **todos** los mensajes de forma independiente, pero los procesa de manera diferente:

```bash
# Terminal 2 — valida pedidos
python consumer.py --group processing

# Terminal 3 — rastrea ingresos por categoría
python consumer.py --group analytics

# Terminal 4 — envía "correos de confirmación"
python consumer.py --group notifications
```

Las tres terminales muestran todos los mensajes, pero con salida diferente — demostrando que cada grupo tiene su propio seguimiento independiente de offsets.

---

### Demo 3: Escalado horizontal

Empezar con 1 consumidor y luego añadir más:

```bash
# Terminal 2 — empieza con TODAS las 6 particiones
python consumer.py --group processing
# → Verás: "PARTICIONES ASIGNADAS: [0, 1, 2, 3, 4, 5]"

# Terminal 3 — añadir un segundo worker → dispara REBALANCEO
python consumer.py --group processing
# → worker-1 ahora tiene [0, 1, 2] y worker-2 tiene [3, 4, 5]

# Terminal 4 — añadir un tercer worker → dispara otro REBALANCEO
python consumer.py --group processing
# → Cada worker ahora tiene 2 particiones

# Prueba Ctrl+C en worker-2 → ¡sus particiones se redistribuyen a los workers restantes!
```

**Observaciones:**
- Cada partición es consumida por exactamente **un** consumidor del grupo.
- Añadir un consumidor → Kafka rebalancea las particiones automáticamente.
- Eliminar un consumidor → sus particiones se reasignan a los supervivientes.
- No puedes tener más consumidores que particiones (los extras quedan inactivos).

---

### Demo 4: *Replays*

Después de producir algunos mensajes, detener el consumidor de analítica y hacer replay de todo para recalcular los totales desde cero:

```bash
# Opción A — Usar el flag --replay (reinicia los offsets)
python consumer.py --group analytics --replay

# Opción B — Resetear offsets externamente, luego reiniciar normalmente
#   Paso 1: Detener todos los consumidores del grupo
#   Paso 2: Resetear offsets
python reset_offsets.py --group analytics
#   Paso 3: Reiniciar
python consumer.py --group analytics

# Opción C — Dry-run para ver offsets actuales sin cambiar nada
python reset_offsets.py --group analytics --dry-run

# Opción D — Resetear a un timestamp específico
python reset_offsets.py --group analytics --timestamp "2026-03-17T18:30:00"
```

---

### Demo 5: Garantías de entrega, acks, idempotencia y commit manual

Entender los compromisos entre velocidad y seguridad tanto en el lado del productor como del consumidor.

#### Productor: `acks` e idempotencia

```bash
# acks=all + idempotente (por defecto) — más seguro, sin pérdida de datos, sin duplicados al reintentar
python producer.py

# acks=1 — solo el líder confirma (riesgo: el líder muere antes de replicar → mensaje perdido)
python producer.py --acks 1

# acks=0 — fire-and-forget (más rápido, pero los mensajes pueden desaparecer silenciosamente)
python producer.py --acks 0
```

**Concepto clave:** con `acks=all` + `enable.idempotence`, el broker asigna a cada
productor un Producer ID + número de secuencia. Si un reintento llega con el mismo (PID, seq),
el broker lo deduplica silenciosamente — sin escrituras dobles, incluso con fallos de red.

#### Consumidor: auto-commit vs commit manual

```bash
# Auto-commit (por defecto) — offsets confirmados por temporizador, antes de terminar el procesamiento
#   → AT-MOST-ONCE: si se cae durante el procesamiento, el mensaje se pierde
python consumer.py --group processing

# Commit manual — offset confirmado DESPUÉS de procesar cada mensaje
#   → AT-LEAST-ONCE: si se cae después de procesar pero antes de confirmar,
#     el mensaje se re-entrega (posible duplicado, pero nunca se pierde)
python consumer.py --group processing --manual-commit
```

**Observaciones clave:**
- At-most-once (auto): rápido, pero puede perder mensajes si se cae.
- At-least-once (manual): seguro, pero tu procesamiento debe manejar duplicados (ser **idempotente**).
- Exactly-once requiere transacciones de Kafka. Más complejo, no se muestra en esta demo.

---

### Demo 6: Pipeline completo

Abrir 6 terminales para ver el panorama completo:

| Terminal | Comando |
|----------|---------|
| 1 | `python producer.py --rate 2` |
| 2 | `python consumer.py --group processing ` |
| 3 | `python consumer.py --group processing ` |
| 4 | `python consumer.py --group analytics` |
| 5 | `python consumer.py --group notifications` |
| 6 | Abrir Redpanda Console en `http://localhost:8090` |

Luego probar:
- Matar un consumidor de processing (Ctrl+C) → Observar cómo worker-1 absorbe sus particiones.
- Reiniciar el consumidor de processing → Las particiones se rebalancean de nuevo.
- Detener analytics-1, ejecutar `python reset_offsets.py --group analytics`, reiniciar → Reproducción completa.

---

## Archivos

| Archivo | Propósito |
|---------|-----------|
| `setup_topic.py` | Crea el topic `orders` con 1 partición por defecto |
| `producer.py` | Genera pedidos aleatorios, con clave por categoría |
| `consumer.py` | Se une a un grupo de consumidores y procesa mensajes |
| `reset_offsets.py` | Resetea los offsets de un grupo para reproducción |

## Comandos útiles de Kafka (vía Docker)

```bash
KAFKA="docker exec -it kafka /opt/kafka/bin"

# Listar topics
$KAFKA/kafka-topics.sh --bootstrap-server localhost:9092 --list

# Describir topic (ver particiones, réplicas, ISR)
$KAFKA/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic orders

# Listar grupos de consumidores
$KAFKA/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list

# Describir un grupo de consumidores (ver lag por partición)
$KAFKA/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group processing
```
