# Spark

## Introducción

Apache Spark es un motor de procesamiento de datos distribuido, diseñado para realizar cálculos en paralelo sobre grandes volúmenes de datos. En esta práctica desplegaremos un clúster Spark en modo *standalone* utilizando Docker Compose, conectándolo a la red del clúster Hadoop desplegado en la práctica anterior.

### Arquitectura objetivo

El clúster Spark se compone de los siguientes nodos:

| Contenedor | Rol | Descripción |
|------------|-----|-------------|
| spark-master | Master | Coordina el clúster y asigna tareas a los workers |
| spark-worker-1 | Worker | Nodo de computación que ejecuta las tareas |
| spark-worker-2 | Worker | Nodo de computación que ejecuta las tareas |
| spark-worker-3 | Worker | Nodo de computación que ejecuta las tareas |

Todos los nodos utilizan la imagen oficial `apache/spark` y se conectan a la misma red Docker que el clúster Hadoop, lo que permite a Spark acceder a los datos almacenados en HDFS.

```
┌─────────────────────────────────────────────────────────────────┐
│                      Red: hadoop_hadoopnet                      │
│                       172.28.1.0/24                             │
│                                                                 │
│  ┌──────────────┐                                               │
│  │ spark-master │  ← Puerto 8080 (Web UI)                       │
│  │  172.28.1.10 │  ← Puerto 7077 (Cluster Manager)              │
│  └──────┬───────┘                                               │
│         │  spark://spark-master:7077                            │
│      ┌──┴──────────────┬──────────────┐                         │
│      │                 │              │                         │
│  ┌───┴────────┐ ┌──────┴──────┐ ┌─────┴───────┐                 │
│  │  worker-1  │ │  worker-2   │ │  worker-3   │                 │
│  │ 172.28.1.11│ │ 172.28.1.12 │ │ 172.28.1.13 │                 │
│  └────────────┘ └─────────────┘ └─────────────┘                 │
│                                                                 │
│  ┌────────────────────────────────────────────┐                 │
│  │         Clúster Hadoop (existente)         │                 │
│  │  master · worker1-4 · history · jupyter    │                 │
│  └────────────────────────────────────────────┘                 │
└─────────────────────────────────────────────────────────────────┘
```

## Requisitos previos

- Tener Docker y Docker Compose instalados.
- Tener el clúster Hadoop de la práctica anterior desplegado y en ejecución (`docker compose up` en la carpeta `hadoop`), ya que Spark se conectará a su red.

## Crear el fichero `compose.yaml`

Dentro de la carpeta `spark`, crea un fichero `compose.yaml` que defina los siguientes servicios:

### Servicio `spark-master`

- **Imagen**: `apache/spark:4.1.1`
- **Nombre del contenedor**: `spark-master`
- **Red**: `hadoopnet` con IP fija `172.28.1.10`

El Master debe exponer los puertos `8080` (Web UI) y `7077` (Cluster Manager). Investiga qué clase Java de Spark arranca el proceso Master y cómo ejecutarla con `spark-class`.

### Servicios `spark-worker-1`, `spark-worker-2` y `spark-worker-3`

- **Imagen**: `apache/spark:4.1.1`
- **Nombres de contenedor**: `spark-worker-1`, `spark-worker-2`, `spark-worker-3`
- **Red**: `hadoopnet` con IPs fijas `172.28.1.11`, `172.28.1.12`, `172.28.1.13`

Los workers deben conectarse al Master mediante la URL `spark://spark-master:7077`. Investiga qué clase Java arranca un Worker y cómo indicarle la dirección del Master.

### Red

La red `hadoopnet` ya existe, creada por el clúster Hadoop. En el `compose.yaml` de Spark hay que declararla como **red externa** referenciando su nombre real: `hadoop_hadoopnet`.

> **Pista**: Usa la propiedad `external: true` y `name:` en la sección `networks`.

## Arrancar el clúster

1. Asegúrate de que el clúster Hadoop está en ejecución.
2. Arranca el clúster Spark:

```bash
cd spark
docker compose up -d
```

3. Verifica que los 4 contenedores están corriendo:

```bash
docker compose ps
```

4. Accede a la interfaz web del Master en: http://localhost:8080

Deberías ver los 3 workers registrados.

## Ejecutar un trabajo de ejemplo

Apache Spark incluye programas de ejemplo en su distribución. Vamos a ejecutar el cálculo de Pi con el algoritmo de Monte Carlo.

Ejecuta el siguiente comando para lanzar el trabajo `SparkPi` distribuyéndolo en el clúster:

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --class org.apache.spark.examples.SparkPi \
  /opt/spark/examples/jars/spark-examples.jar 10000
```

Mientras se ejecuta, abre la interfaz web del Master (http://localhost:8080) y observa cómo aparece la aplicación en la sección *Running Applications*.


## Interfaces web

| Servicio | URL |
|----------|-----|
| Spark Master | http://localhost:8080 |

> **Nota**: Los workers no exponen puertos al host, pero sus interfaces web son accesibles a través del enlace que aparece en la UI del Master, siempre que se acceda desde un contenedor de la misma red. Revisa la carpeta firefox o webtop de este repo.