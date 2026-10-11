# Airflow + Spark — skeleton plug & play (Docker Compose)

Esqueleto de proyecto, listo para clonar y empezar a escribir pipelines. Trae toda la infraestructura resuelta y **ningún DAG ni job**: `dags/` y `spark/` vienen vacíos.

- **Apache Airflow 3.3.1** (CeleryExecutor) — API server, scheduler, dag-processor, triggerer y **1 worker dedicado** (1 CPU / 4 GB RAM).
- **Apache Spark 4.1.3** en modo standalone — 1 master + 2 workers (escalable).
- **PostgreSQL 16** (metadata DB) y **Redis 7.2** (broker de Celery).
- Imagen custom de Airflow con Java 17 + binarios de Spark ya instalados, para que `SparkSubmitOperator` funcione sin configuración extra.
- Soporte opcional para **Google Cloud** (GCS + BigQuery) desde Airflow y desde Spark.

## Requisitos

- Docker >= 24 y Docker Compose v2 (`docker compose`, no `docker-compose`).
- `make`, `curl` y `openssl` en el host.
- Linux/macOS/WSL2. Con la config por defecto el stack pide en pico ~6.2 cores / ~17 GB (ver "Cómputo total"); bajá los límites en `.env` para máquinas más chicas.

## Estructura

```
.
├── docker-compose.yaml     # todo el stack
├── Dockerfile               # imagen custom de Airflow (+Java +Spark client)
├── spark.Dockerfile         # imagen custom de Spark (+pandas/pyarrow/numpy en los executors)
├── requirements.txt         # deps Python instaladas sobre la imagen de Airflow
├── Makefile                 # comandos plug-and-play
├── .env.example             # TODAS las versiones y parámetros configurables (`make init` genera .env)
├── spark-conf/              # spark-defaults.conf (jars + auth de GCS/BigQuery)
├── dags/                    # tus DAGs de Airflow
├── spark/                   # tus jobs PySpark (montados en /opt/spark-apps)
│   └── jars/                   # conectores descargados por `make gcp-jars` (gitignored)
├── src/                     # código Python compartido entre DAGs y jobs Spark (montado en /opt/src)
├── sql/                     # SQL compartido entre DAGs y jobs Spark (montado en /opt/sql)
│   ├── transformations/        # SQL de procesos ELT / queries que usa Spark
│   └── ddl/                    # DDL de tablas y datasets
├── data/                    # salida de los jobs (montado en /opt/spark-data, gitignored)
├── gcp/                     # tu service account key.json (gitignored)
├── plugins/                 # plugins custom de Airflow
├── logs/                    # logs de Airflow (montado como volumen)
├── config/                  # airflow.cfg custom si lo necesitas
├── notebooks/               # notebooks de experimentación (VS Code + kernel local)
└── local-requirements.txt   # deps de un venv local (autocompletado + kernel de notebooks)
```

## Arranque rápido

```bash
git clone <tu-repo> mi-proyecto
cd mi-proyecto

make init      # crea .env (con Fernet key y JWT secret generados), baja los jars, migra la DB y crea el usuario admin
make up        # construye las imágenes y levanta todo el stack en background
make ps        # verifica que todos los servicios estén healthy
```

- Airflow UI → http://localhost:8080 (usuario/clave por defecto: `airflow` / `airflow`, definidos en `.env`)
- Spark Master UI → http://localhost:8081
- Spark Application UI (por job, mientras corre) → http://localhost:4040
- Flower (monitor de Celery, opcional) → `make flower` → http://localhost:5555

## Comandos del Makefile

| Comando | Qué hace |
|---|---|
| `make build` | Solo construye las imágenes custom |
| `make init` | Setup inicial (`.env`, dirs, jars, migración DB, usuario admin) |
| `make up` | Levanta todo el stack |
| `make down` | Detiene todos los contenedores |
| `make restart` | `down` + `up` |
| `make ps` | Estado/health de los servicios |
| `make logs` | Sigue los logs de todo el stack |
| `make clean` | Detiene y **borra volúmenes** (incluida la metadata DB de Airflow) |
| `make reset` | `clean` + `init` + `up` — resetea todo desde cero |
| `make flower` | Levanta Flower (monitor de Celery) |
| `make airflow-shell` | Shell dentro del contenedor del API server |
| `make spark-shell` | Spark shell interactivo contra el cluster |
| `make spark-submit JOB=mi_job.py` | Envía `spark/mi_job.py` directo al cluster (sin pasar por Airflow) |
| `make gcp-jars` | Descarga los conectores de GCS/BigQuery (ya lo llaman `init` y `up`) |

## Dónde va cada cosa

| Qué | Dónde | Ruta dentro de los contenedores |
|---|---|---|
| DAGs | `dags/*.py` | `/opt/airflow/dags` |
| Jobs PySpark | `spark/*.py` | `/opt/spark-apps` |
| Código compartido | `src/*.py` | `/opt/src` (`from src.modulo import funcion`) |
| SQL | `sql/**/*.sql` | `/opt/sql` |
| Output local de los jobs | `data/` | `/opt/spark-data` |

Todo está montado como volumen: los cambios se recogen sin rebuild ni restart (el dag-processor escanea `dags/` cada ~30s).

`src/` y `sql/` se montan en **todos** los contenedores de Airflow y en `spark-master`/`spark-worker`, con `PYTHONPATH=/opt` en ambos lados, así que `from src.modulo import funcion` resuelve igual desde un DAG, desde el driver de Spark y desde una UDF que corre en los executors.

## Conectar Airflow con Spark

La conexión `spark_default` se crea automáticamente vía la variable de entorno:

```
AIRFLOW_CONN_SPARK_DEFAULT=spark://spark-master:7077?deploy_mode=client
```

Úsala en tus DAGs con `SparkSubmitOperator(conn_id="spark_default", application="/opt/spark-apps/tu_job.py")`.

Los jobs que escriban a `/opt/spark-data` necesitan estas dos configs en su `SparkSession` (el porqué está en `CLAUDE.md`, punto 4 de "Decisiones no obvias"):

```python
.config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
.config("spark.hadoop.fs.permissions.umask-mode", "000")
```

## Google Cloud (GCS + BigQuery) — opcional

- **Airflow**: provider `apache-airflow-providers-google` en `requirements.txt`, con la conexión `google_cloud_default` ya creada (`AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT`).
- **Spark**: conectores `gcs-connector` (lee/escribe `gs://...`) y `spark-4.1-bigquery` (tablas de BigQuery como DataFrame), descargados por `make gcp-jars` a `spark/jars/` y cargados automáticamente vía `spark-conf/spark-defaults.conf`.
- **Credenciales**: poné tu service account key en `./gcp/key.json` (gitignored). Se monta de solo lectura en `/opt/gcp` en Airflow, `spark-master` y `spark-worker`. No hace falta declarar credenciales en el código de los jobs.

Al escribir a BigQuery, usá siempre `writeMethod=direct` (el método indirecto por defecto exige un bucket temporal de GCS que este stack no configura):

```python
df.write.format("bigquery") \
  .option("table", "tu-proyecto.tu_dataset.tu_tabla") \
  .option("writeMethod", "direct") \
  .mode("overwrite").save()
```

Si cambiás las versiones de los conectores, actualizá `GCS_CONNECTOR_VERSION` / `BQ_CONNECTOR_VERSION` en el `Makefile` **y** los nombres de archivo en `spark-conf/spark-defaults.conf` (deben coincidir).

Si no usás Google Cloud no hay que tocar nada: sin `gcp/key.json` el stack levanta igual y los jobs de Spark puro funcionan.

## Venv local y notebooks

Para autocompletado en el IDE y para correr notebooks desde VS Code (sin servidor de Jupyter):

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r local-requirements.txt \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.1/constraints-3.10.txt"
pip install --force-reinstall --no-deps pyspark==4.1.3
```

El último paso evita un conflicto entre `pyspark` y `pyspark-client` (ver `CLAUDE.md`, punto 11). Para correr Spark en el host hace falta además un JDK 17+ (punto 12).

## Cambiar versiones

Todo está centralizado en `.env`:

```env
AIRFLOW_VERSION=3.3.1
SPARK_VERSION=4.1.3
PYTHON_VERSION=3.10
```

⚠️ **`PYTHON_VERSION` debe coincidir con la versión de Python que trae la imagen oficial de `apache/spark`** (actualmente 3.10 en la imagen `-ubuntu`). Si no coinciden, Spark falla con `PYTHON_VERSION_MISMATCH` al ejecutar cualquier tarea PySpark. Antes de cambiar `SPARK_VERSION`, verificá qué Python trae esa imagen nueva.

Al cambiar `SPARK_VERSION`, actualizá también el pin de `pyspark==` en `requirements.txt` y `local-requirements.txt`. Luego:

```bash
make down
make up   # reconstruye las imágenes automáticamente por el --build
```

## Cómputo total y afinado por máquina

Con la config por defecto, el stack pide en pico aproximadamente **~6.2 cores / ~17 GB de RAM**:

| Servicio | CPU | RAM |
|---|---|---|
| 1x Airflow worker | 1 | 4 GB |
| 2x Spark worker | 1 c/u | 4 GB c/u |
| spark-master | 1 | 1 GB |
| airflow-apiserver | 0.5 | 1 GB |
| airflow-scheduler | 0.5 | 1 GB |
| airflow-dag-processor | 0.3 | 512 MB |
| airflow-triggerer | 0.2 | 512 MB |
| postgres | 0.5 | 1 GB |
| redis | 0.2 | 256 MB |

Todos estos límites viven en `.env` (`*_CPU_LIMIT` / `*_MEM_LIMIT`, `SPARK_WORKER_CORES` / `SPARK_WORKER_MEMORY`), calibrados para un host de **8 cores / 32 GB**. Ajustá los valores ahí — no hace falta tocar `docker-compose.yaml`. Si cambiás `SPARK_WORKER_MEMORY`, ajustá también `spark.executor.memory` en `spark-conf/spark-defaults.conf`.

Para más de un worker de Airflow, definí un servicio `airflow-worker-2` en `docker-compose.yaml` usando `<<: *airflow-worker` como ancla (así cada uno conserva sus propios límites).

## Notas de seguridad

Este stack está pensado para desarrollo local, igual que el `docker-compose.yaml` oficial de Airflow. Antes de usarlo en producción:

- Cambiá `_AIRFLOW_WWW_USER_PASSWORD` y las credenciales de Postgres (`make init` ya genera una Fernet key y un JWT secret propios en tu `.env`).
- No expongas los puertos 8080/8081/4040/5555/7077 directamente a internet sin un proxy/autenticación adicional.
- `gcp/key.json` es una credencial real — está gitignored, pero tratala como cualquier secreto y dale al service account solo los permisos IAM mínimos que tus jobs necesiten.
- Considerá Kubernetes + el Helm chart oficial de Airflow para producción real.

## Troubleshooting

- **"not enough memory"**: subí la memoria asignada a Docker Desktop (Settings → Resources) o bajá los límites en `.env`.
- **Workers en `unhealthy`**: revisá `make logs` — normalmente es que Redis/Postgres aún no terminaron de levantar; `airflow-init` debe completarse antes de que los demás arranquen.
- **`SparkSubmitOperator` falla con "class not found" o error de red**: confirmá que `spark-master` está healthy (`make ps`) y que `pyspark` en `requirements.txt` coincide con `SPARK_VERSION`.
- **`PYTHON_VERSION_MISMATCH`**: `PYTHON_VERSION` en `.env` no coincide con el Python de la imagen de `apache/spark`. Corregilo y corré `make down && make up`.
- **`Unable to clear output directory ...` / `Mkdirs failed to create ...` / `Permission denied` en `data/`**: al job le faltan las dos configs de la sección "Conectar Airflow con Spark". Si quedaron carpetas huérfanas de `root` de una corrida vieja, limpialas una vez con `sudo rm -rf data/*` y volvé a correr `make up`.
- **Un DAG nuevo "no aparece" en la UI**: confirmá con `airflow dags list` desde `make airflow-shell` antes de asumir un error — la UI cachea agresivamente.

Más contexto sobre el porqué de cada decisión de infraestructura en `CLAUDE.md`.
