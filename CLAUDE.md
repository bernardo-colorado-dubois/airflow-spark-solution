# CLAUDE.md

Contexto de proyecto para Claude Code. Este repo es un **skeleton**: un stack local de **Apache Airflow 3.3.1 + Apache Spark 4.1.3** vía Docker Compose, orquestado con `Makefile`, sin DAGs ni jobs. La infraestructura ya fue validada de punta a punta (Airflow → Spark, GCS, BigQuery); lo que falta es el código del proyecto que se construya encima.

## Qué es esto

Plug-and-play para desarrollar pipelines de datos: DAGs de Airflow que envían jobs a un cluster de Spark standalone (1 master + 2 workers), todo en contenedores. CeleryExecutor con 1 worker dedicado (1 CPU / 4GB RAM). Pensado para desarrollo local, no para producción real (ver sección de seguridad al final).

## Arquitectura / servicios (`docker-compose.yaml`)

- **postgres** — metadata DB de Airflow
- **redis** — broker de Celery
- **spark-master** / **spark-worker** (x2) — cluster Spark standalone, corren como `root` con `umask 000` (ver "Decisiones no obvias" abajo)
- **airflow-apiserver** — UI + API (puerto 8080)
- **airflow-scheduler**, **airflow-dag-processor**, **airflow-triggerer** — procesos de control de Airflow 3.x
- **airflow-worker-1** — Celery worker, 1 CPU / 4GB RAM (límite duro vía `deploy.resources.limits`). Acá corre el **driver** de Spark (`deploy_mode=client`).
- **airflow-init** — servicio de setup (migración DB, usuario admin), corre una vez y termina
- **flower** (perfil `flower`) — monitor de Celery, opcional

Todos los servicios de Airflow comparten config vía el YAML anchor `x-airflow-common`.

Los contenedores de Airflow, `spark-master` y `spark-worker` tienen además `gcp/` montado en `/opt/gcp` y `GOOGLE_APPLICATION_CREDENTIALS` seteada, para hablar con GCS/BigQuery (ver sección "Google Cloud" abajo).

## Comandos (Makefile)

```bash
make init              # primera vez: crea .env, dirs, baja jars, migra DB, crea usuario admin
make up                 # build + levanta todo el stack (aplica chmod 777 a data/spark primero)
make down                # detiene todo
make restart              # down + up
make ps                    # estado/health de todos los servicios
make logs                   # logs de todo el stack
make clean                   # down -v (BORRA volúmenes, incluida la metadata DB)
make reset                    # clean + init + up (reset total)
make flower                    # levanta Flower en :5555
make airflow-shell               # shell dentro de airflow-apiserver
make spark-shell                  # spark-shell interactivo contra el cluster
make spark-submit JOB=mi_job.py    # corre spark/mi_job.py directo, sin pasar por Airflow
make gcp-jars                       # descarga los jars de GCS/BigQuery para Spark (idempotente, ya lo llaman init/up)
```

## Archivos clave

| Archivo | Para qué |
|---|---|
| `.env` | **Única fuente de verdad para versiones y recursos.** Cambiar aquí, no en `docker-compose.yaml`. Gitignored; lo genera `make init` a partir de `.env.example` (con Fernet key y JWT secret propios). Variables nuevas del proyecto van en ambos archivos. |
| `docker-compose.yaml` | Definición de todos los servicios |
| `Dockerfile` | Imagen custom de Airflow: base `apache/airflow` + JDK 17 + binarios de Spark (para que `spark-submit` funcione desde el driver) |
| `spark.Dockerfile` | Imagen custom de `spark-master`/`spark-worker`: base `apache/spark` (`SPARK_IMAGE_NAME`) + pandas/pyarrow/numpy, necesarios en los executors para jobs con `applyInPandas` (ver "Decisiones no obvias", punto 10) |
| `requirements.txt` | Deps Python sobre la imagen de Airflow (`apache-airflow-providers-apache-spark`, `apache-airflow-providers-google`, `pyspark`) |
| `dags/` | DAGs de Airflow (montado como volumen, hot-reload en ~30s sin rebuild) |
| `spark/` | Jobs PySpark (montado como volumen en ambos: Airflow y Spark, `/opt/spark-apps`, hot-reload sin rebuild) |
| `src/` | Paquete Python compartido entre DAGs de Airflow y jobs de Spark. Montado como `/opt/src` en **todos** los contenedores de Airflow (`x-airflow-common`) y en `spark-master`/`spark-worker`, con `PYTHONPATH=/opt` seteado en ambos para que `from src.modulo import funcion` resuelva igual desde un DAG (`@task`/`PythonOperator`) que desde un job PySpark. En Spark, esto cubre tanto el driver (hereda `PYTHONPATH` del contenedor donde corre `spark-submit`) como los executors de `spark-worker` — para estos últimos hace falta además `spark.executorEnv.PYTHONPATH=/opt` en `spark-conf/spark-defaults.conf`, porque son procesos Python separados que lanza la JVM y no heredan el entorno del contenedor automáticamente. Hot-reload sin rebuild. |
| `spark/jars/` | Jars de conectores Spark (GCS, BigQuery) descargados por `make gcp-jars`. Gitignored (binarios pesados), no versionar. |
| `sql/` | Carpeta compartida de SQL, montada como `/opt/sql` en los mismos contenedores que `src/`. Dos subcarpetas: `sql/transformations/` (SQL de procesos ELT y queries que consume Spark) y `sql/ddl/` (DDL de tablas/datasets). |
| `spark-conf/spark-defaults.conf` | Config global de Spark montada en `/opt/spark/conf/spark-defaults.conf` (driver + `spark-master`). Ahí vive `spark.jars` y la auth de GCS/BigQuery. **Es bind mount de un solo archivo** — si lo editás con una herramienta que reescribe el archivo (nuevo inode) en vez de modificarlo in-place, el contenedor sigue viendo la versión vieja hasta recrearlo (`docker compose up -d --force-recreate spark-master spark-worker airflow-worker-1`). |
| `gcp/key.json` | Service account de GCP. Gitignored, nunca commitear. Montado de solo lectura en Airflow, `spark-master` y `spark-worker`. |
| `data/` | Output de los jobs (montado como `/opt/spark-data`). Gitignored. |
| `venv/` + `local-requirements.txt` | venv local para el host (fuera de Docker): autocompletado del IDE + kernel de Jupyter para `notebooks/`. Gitignored. **No** se usa dentro de los contenedores. |
| `notebooks/` | Notebooks de experimentación, corridos desde VS Code con el kernel del `venv/` local — sin servidor de Jupyter. |

## Conexión Airflow ↔ Spark

Se crea automáticamente al iniciar vía variable de entorno:
```
AIRFLOW_CONN_SPARK_DEFAULT=spark://spark-master:7077?deploy_mode=client
```
Úsala en DAGs con `SparkSubmitOperator(conn_id="spark_default", application="/opt/spark-apps/tu_job.py")`.

## Google Cloud (GCS + BigQuery)

Opcional: sin `gcp/key.json` el stack levanta igual y Spark puro funciona.

- Provider de Airflow: `apache-airflow-providers-google`. La conexión `google_cloud_default` se crea por env var (`AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT: 'google-cloud-platform://'`), sin extra a propósito: el provider resuelve por ADC vía `GOOGLE_APPLICATION_CREDENTIALS`, pero el hook exige igual que la Connection exista (`AirflowNotFoundException` si falta).
- Conectores de Spark: `gcs-connector` + `spark-4.1-bigquery` (jars en `spark/jars/`, cargados vía `spark-conf/spark-defaults.conf`). La auth sale de ese archivo; los jobs no declaran credenciales (ver punto 6).
- Escrituras a BigQuery: siempre `.option("writeMethod", "direct")` (ver punto 7).
- BigQuery no crea datasets implícitamente al escribir una tabla: crearlos de antemano o desde el DAG/job.

## Versiones actuales (`.env`)

- `AIRFLOW_VERSION=3.3.1`
- `SPARK_VERSION=4.1.3`
- `PYTHON_VERSION=3.10` — **debe coincidir con la versión de Python que trae la imagen oficial `apache/spark`**, no es arbitrario (ver punto 3).

## Decisiones no obvias / troubleshooting ya resuelto

Este stack pasó por varias vueltas de debugging real. Si algo de esto reaparece, ya se conoce la causa — no reintroducir los fixes descartados:

1. **`spark-master`/`spark-worker` corren como `root`, NO como `AIRFLOW_UID`.**
   Se intentó unificar el UID de los contenedores de Spark con el de Airflow (`user: "${AIRFLOW_UID}:0"`) para evitar conflictos de permisos al escribir output — **rompió el arranque de Spark** (`java.io.IOException: Invalid UID, could not determine effective user`, porque la imagen oficial `apache/spark` no soporta UIDs arbitrarios sin entrada en `/etc/passwd`). **No repetir ese cambio.**

2. **En su lugar, el fix de permisos tiene tres partes:**
   - `entrypoint` de `spark-master`/`spark-worker` antepone `umask 000` antes de lanzar `spark-class`. Esto cubre archivos que Spark/la JVM escriben con syscalls directas del SO (logs, `work/`, temporales), pero **no** cubre los datos que el job escribe a `/opt/spark-data` — ver punto 4, es un mecanismo aparte.
   - `make init`/`make up` aplican `chmod -R 777 data spark` en el **host** antes de levantar, porque el driver (dentro de `airflow-worker`) corre con un UID distinto al de Spark y necesita poder crear/borrar ahí también. Esto solo corre una vez al levantar el stack — no repara permisos que un job deje mal en corridas posteriores (para eso, punto 4).
   - Si cambias esta lógica, hacelo en el `Makefile`, no asumas que el `umask` del entrypoint de Spark alcanza para los archivos de datos.

3. **`PYTHON_VERSION` en `.env` debe coincidir con el Python que trae la imagen `apache/spark` usada** (actualmente 3.10). Si no coinciden: `PySparkRuntimeError: [PYTHON_VERSION_MISMATCH] Python in worker has different version: X than that in driver: Y`. Al cambiar `SPARK_VERSION`, revisar qué Python trae esa imagen nueva antes de tocar `PYTHON_VERSION`.

4. **Jobs PySpark que escriben a `/opt/spark-data` deben usar el commit algorithm v2 y forzar el umask de Hadoop a 0:**
   ```python
   .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
   .config("spark.hadoop.fs.permissions.umask-mode", "000")
   ```
   - **Algorithm v2**: sin esto, el commit protocol v1 (default) puede fallar con `IOException: Failed to rename ...` al mover archivos de `_temporary/` a su ubicación final — problema conocido del `rename()` sobre volúmenes bind-mounted de Docker, independiente de permisos.
   - **`fs.permissions.umask-mode=000`**: Spark no escribe a `file://` con syscalls crudas del SO, sino vía la capa `FileSystem` de Hadoop (`RawLocalFileSystem`), que tiene su **propio umask lógico** (`fs.permissions.umask-mode`, default `022`) aplicado con `setPermission()` explícito **después** de crear cada archivo/directorio — pisa lo que haya hecho el `umask 000` del entrypoint de Spark (punto 2), que no tiene ningún efecto acá. Sin este fix, todo lo escrito en `/opt/spark-data` queda `755`/`644` propiedad de `root`, y el driver de Airflow (UID distinto) no puede borrarlo/sobreescribirlo en la próxima corrida → `SparkException: Unable to clear output directory ... prior to writing to it` (con `WARN FileUtil: Failed to delete ... it still exists` antes).

   Replicar ambos configs en todo job que persista a disco local. Un `coalesce(1)` antes de escribir reduce además la cantidad de renames.

5. **La UI de Airflow cachea agresivamente.** Si un DAG nuevo "no aparece" pero `airflow dags list` por CLI sí lo muestra, es casi siempre refresh/filtro de UI, no un bug real. Confirmar siempre por CLI antes de asumir que el dag-processor falló.

6. **El `gcs-connector` (probado en 4.0.5) no resuelve bien Application Default Credentials dentro de estos contenedores** — a pesar de tener `GOOGLE_APPLICATION_CREDENTIALS=/opt/gcp/key.json` seteada y el archivo legible, cae a buscar el metadata server de Compute Engine y falla con `ComputeEngineCredentials cannot find the metadata server`. **Fix**: en `spark-conf/spark-defaults.conf` la auth queda explícita —
   ```
   spark.hadoop.fs.gs.auth.type                         SERVICE_ACCOUNT_JSON_KEYFILE
   spark.hadoop.fs.gs.auth.service.account.json.keyfile /opt/gcp/key.json
   ```
   No volver a implicit-ADC para el conector GCS sin re-probar contra un bucket real. El provider de Airflow (`apache-airflow-providers-google`) sí resuelve bien ADC vía la misma env var — el problema es específico del `gcs-connector`, no de la clave ni del montaje.

   Para BigQuery, la misma auth explícita se resuelve **globalmente** en `spark-conf/spark-defaults.conf` con `spark.hadoop.google.cloud.auth.service.account.json.keyfile=/opt/gcp/key.json`. No hace falta `.option("credentialsFile", ...)` por job.

7. **Escribir a BigQuery sin `.option("writeMethod", "direct")` falla con `IllegalArgumentException: Either temporary or persistent GCS bucket must be set`.** El conector `spark-4.1-bigquery` usa por default el método de escritura indirecto, que necesita un bucket temporal de GCS — no configurado en este stack. Cualquier job que escriba a BigQuery necesita esa opción explícita en el `.write.format("bigquery")`. Además, `writeMethod=direct` + `mode("overwrite")` no puede alterar el tipo de una columna existente (`InvalidSchemaException: Incompatible type for field`): si cambia el schema hay que borrar la tabla y dejar que el job la recree. El conector tampoco cubre `MERGE`/upserts — para eso, escribir a una tabla de staging y correr el `MERGE` con el cliente `google-cloud-bigquery` desde el driver.

8. **Los logs de contenedor están capeados** (`x-logging` en `docker-compose.yaml`: `json-file`, `max-size: 10m`, `max-file: 3` → 30MB tope por contenedor), así un `SparkSubmitOperator` con `verbose=True` corriendo repetidamente nunca puede llenar el disco por sí solo. Si el disco del host se llena (`No space left on device` / `Failed to create a temp directory`), revisar primero `docker system df` — la causa habitual es acumulación de imágenes y build cache de Docker, no este stack.

9. **Las libs de `requirements.txt` solo existen en el driver, no en los executors.** `spark-master`/`spark-worker` no instalan `requirements.txt`; solo la imagen de Airflow (donde corre el driver) lo hace. Cualquier lógica que use una librería Python de terceros (`requests`, clientes de APIs, `google-cloud-bigquery`, etc.) debe correr en el driver (loop simple, o `spark.createDataFrame` sobre una lista ya construida en Python), nunca dentro de una UDF/`mapPartitions` que se ejecute en los workers → `ModuleNotFoundError` ahí. Si hay que paralelizar de verdad ese trabajo, agregar la lib a `spark.Dockerfile` (punto 10).

10. **`spark-master`/`spark-worker` corren una imagen custom (`spark.Dockerfile`, tag `SPARK_CUSTOM_IMAGE_NAME` en `.env`) en vez de `apache/spark` pelada.** La imagen oficial no trae pandas/pyarrow/numpy, y `applyInPandas`/pandas UDFs corren en los **executors**. `spark.Dockerfile` solo agrega esas tres libs (pineadas a las versiones que resuelve `requirements.txt` en el driver, para que Arrow serialice igual de ambos lados) sobre `SPARK_IMAGE_NAME`. `make up`/`make init` ya lo buildean solo; si se toca `spark.Dockerfile`, recrear con `docker compose up -d --build spark-master spark-worker`. No agregar más libs sin evaluar antes si la lógica se puede reescribir con Window functions puras o mover al driver.

11. **En el venv local, `pyspark` y `pyspark-client` se pisan** (`ImportError: cannot import name 'SparkExecutorInfo' from 'pyspark.core.status'`). `pyspark-client` (dependencia de `apache-airflow-providers-apache-spark`, thin client de Spark Connect) instala archivos bajo el mismo namespace `pyspark/` y deja un híbrido roto. **Fix**: `pip install --force-reinstall --no-deps pyspark==4.1.3` después de cada `pip install -r local-requirements.txt`.

12. **Spark 4.1.3 no arranca con Java 11** (`UnsupportedClassVersionError: ... class file version 61.0 ...`) **— necesita Java 17+.** Dentro de los contenedores no es problema (`Dockerfile` instala Temurin 17 y la imagen de Spark ya trae Java 17), pero para correr Spark en el host (notebooks, `local[*]`) el `java` default puede resolver a una versión vieja aunque haya un JDK 17+ instalado en paralelo: setear `JAVA_HOME` al JDK 17+ en el proceso del kernel antes de crear la `SparkSession`.

## Convenciones para agregar trabajo nuevo

- **DAGs nuevos** → `dags/*.py`. Se recogen solos (dag-processor escanea cada ~30s), no requieren rebuild ni restart.
- **Jobs PySpark nuevos** → `spark/*.py`. Mismo trato: hot-reload sin rebuild. Si escriben a `/opt/spark-data`, replicar el fix del punto 4 arriba.
- **Jobs PySpark que leen/escriben GCS o BigQuery** → no declarar credenciales en el código, la auth ya sale de `spark-conf/spark-defaults.conf` (punto 6). Escrituras a BigQuery con `writeMethod=direct` (punto 7).
- **Código compartido entre DAGs y jobs de Spark** → `src/*.py`. Importar como `from src.modulo import funcion` desde ambos lados; en Spark funciona tanto en el driver como dentro de UDFs que corren en los executors.
- **SQL** → `sql/transformations/` o `sql/ddl/`, leído desde `/opt/sql/...` en DAGs y jobs.
- **Nueva dependencia Python para Airflow/DAGs/driver** → agregar a `requirements.txt` (y a `local-requirements.txt` si se quiere en el IDE), requiere `make down && make up` (rebuild de imagen). Para executors, ver puntos 9 y 10.
- **Nuevo conector Spark (jar)** → agregarlo a `make gcp-jars` (o un target nuevo) en el `Makefile`, no bajarlo a mano; sumar el path a `spark.jars` en `spark-conf/spark-defaults.conf` y recrear los contenedores que lo montan (ver caveat de bind-mount de un solo archivo en "Archivos clave").
- **Cambiar recursos** (CPU/RAM de cualquier servicio) → todo vive en `.env` como variables `*_CPU_LIMIT`/`*_MEM_LIMIT`, no hardcodear en `docker-compose.yaml`.
- **Variables de entorno del proyecto** (API keys, nombres de dataset, etc.) → `.env` + placeholder en `.env.example`. Llegan a los contenedores de Airflow (DAGs y driver de Spark) vía `env_file`, **no** a `spark-master`/`spark-worker`.
- **Notebooks nuevos** → `notebooks/*.ipynb`, kernel del `venv/` local. Una sola `SparkSession` por notebook: una vez creada, su config (jars, auth) queda fija para esa JVM y un `getOrCreate()` posterior ignora config nueva — reiniciar el kernel para cambiarla.

## Recursos (calibrado para host de 8 cores / 32GB)

Pico estimado bajo carga completa: ~6.2 cores / ~17GB. Ver tabla completa y cómo ajustar por `.env` en `README.md`.

## Seguridad (solo para dev local)

Credenciales default (`airflow`/`airflow`, Postgres) quedan en `.env` para plug-and-play; la Fernet key y el JWT secret los genera `make init`. **No usar así en producción** — ver sección "Notas de seguridad" en `README.md` antes de exponer esto fuera de `localhost`.
