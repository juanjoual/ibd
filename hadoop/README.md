# Infraestructura Big Data

Basado en:
- [hpcjmart/hadoop-docker](https://github.com/hpcjmart/hadoop-docker).

## Arquitectura

| Contenedor | Rol | Descripción |
|------------|-----|-------------|
| master | NameNode + ResourceManager | Nodo maestro HDFS y gestor de recursos YARN |
| worker1–4 | DataNode + NodeManager | Nodos de almacenamiento HDFS y computación YARN |
| history | MapReduce History Server | Interfaz web para logs de trabajos MapReduce completados |
| jupyter | Jupyter Notebook | Notebook interactivo con herramientas cliente de Hadoop |

## Inicio rápido

Construir las imágenes e iniciar el clúster:

```bash
make
docker compose up
```

Para detener el clúster:

```bash
docker compose down
```

Para detener y eliminar todos los volúmenes de datos:

```bash
docker compose down -v
```

## Interfaces web

| Servicio | URL |
|----------|-----|
| YARN ResourceManager | http://localhost:8088 |
| HDFS NameNode | http://localhost:9870 |
| MapReduce HistoryServer | http://localhost:19888 |
| Spark UI (durante trabajos) | http://localhost:4040 |
| DataNode 1 | http://localhost:9864 |
| DataNode 2 | http://localhost:9865 |
| DataNode 3 | http://localhost:9866 |
| DataNode 4 | http://localhost:9867 |
| NodeManager 1 | http://localhost:8042 |
| NodeManager 2 | http://localhost:8043 |
| NodeManager 3 | http://localhost:8044 |
| NodeManager 4 | http://localhost:8045 |
| Jupyter Notebook | http://localhost:8888/lab |

