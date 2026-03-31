# Infrastructure Specification

## Architecture Overview

Three modular Docker Compose files connected via a shared network. Only Airflow is started manually — DAGs manage all other infrastructure.

### How Docker Engine works

All containers are managed by a single **Docker Engine** (daemon process) running on the host machine.
Commands like `docker compose up` don't create containers directly — they send requests to Docker Engine via a **Unix socket file** (`/var/run/docker.sock`).

```
Developer terminal                    Docker Engine (daemon)
      │                                      │
      ├── docker compose up ──────────────►  │ ← listens on docker.sock
      │   (Docker CLI)        via socket     │
      │                                      ├── creates containers
      │                                      ├── creates networks
      │                                      ├── mounts volumes
      │                                      └── manages lifecycle
```

This is how Airflow worker can control infrastructure: it has Docker CLI installed and the host's `docker.sock` mounted inside the container. When the worker runs `up.sh postgres`, Docker CLI sends the request through the mounted socket to the **same Docker Engine** on the host — creating sibling containers (not nested ones).

### Step 1: Manual Startup — `./infra/up.sh airflow`

The only manual step. Creates Airflow and all its internal services.

```mermaid
flowchart TB
    classDef iface    fill:#dbeafe,stroke:#2563eb,color:#1e3a8a
    classDef cls      fill:#dcfce7,stroke:#16a34a,color:#14532d
    classDef abs      fill:#fef3c7,stroke:#d97706,color:#78350f

    USER["👤 Developer<br/>Terminal"]
    UPSH[["«script»<br/>up.sh airflow"]]:::abs
    CONFIG[["«config»<br/>.env"]]:::abs
    SOCK(["docker.sock"]):::iface

    subgraph ENGINE["Docker Engine"]
        subgraph Airflow["📁 airflow — created"]
            ADB(["airflow-db :5433"]):::iface
            REDIS(["giljobi-redis :6379"]):::iface
            INIT["airflow-init"]:::cls
            SCH["airflow-scheduler"]:::cls
            WS["airflow-webserver :8090"]:::cls
            AW["airflow-worker<br/>+ Docker CLI<br/>+ docker.sock mount"]:::cls
        end
        NET{{"giljobi-network<br/>created"}}
    end

    USER -- "run" --> UPSH
    CONFIG -. "read" .-> UPSH
    UPSH -- "docker compose up" --> SOCK
    SOCK -- "create" --> Airflow
    SOCK -- "create" --> NET

    style ENGINE fill:none,stroke:#888,stroke-width:2px,stroke-dasharray:5,color:#888
    linkStyle 0 stroke:#f97316,stroke-width:2px
    linkStyle 1 stroke:#eab308,stroke-width:2px,stroke-dasharray:5
    linkStyle 2 stroke:#ec4899,stroke-width:2px
    linkStyle 3 stroke:#22c55e,stroke-width:2px
    linkStyle 4 stroke:#0d9488,stroke-width:2px
```

### Step 2a: DAG Execution — Market Trend Stream

When `market_trend_pipeline` DAG is triggered. Spark is **not used** — only DB.

```mermaid
flowchart TB
    classDef iface    fill:#dbeafe,stroke:#2563eb,color:#1e3a8a
    classDef cls      fill:#dcfce7,stroke:#16a34a,color:#14532d
    classDef abs      fill:#fef3c7,stroke:#d97706,color:#78350f
    classDef contract fill:#fce7f3,stroke:#db2777,color:#831843

    subgraph ENGINE["Docker Engine"]

    subgraph Airflow["📁 giljobi-airflow — already running"]
        WS["airflow-webserver :8090"]:::cls
        SCH["airflow-scheduler"]:::cls
        REDIS(["giljobi-redis :6379"]):::iface
        AW["airflow-worker + Docker CLI"]:::cls
    end

    SOCK(["docker.sock"]):::iface
    UPSH[["up.sh postgres"]]:::abs

    subgraph Storage["📁 market-trend-db — created by DAG"]
        PG(["PostgreSQL 17 :5432"]):::iface
    end

    NET{{"giljobi-network"}}

    end

    C_ENSURE{{"«contract» ensure_db<br/>Start DB if not running<br/>Never stop (data persists)"}}:::contract
    PIPELINE["Pipeline Tasks<br/>noc_setup → scrape → download<br/>→ validate → transform → load"]

    WS -- "1. trigger" --> SCH
    SCH -- "2. dispatch" --> REDIS
    REDIS -- "3. pick up" --> AW
    AW -- "4. up.sh postgres" --> UPSH
    UPSH -- "5. docker compose via" --> SOCK
    SOCK --> C_ENSURE
    C_ENSURE -- "6. start/verify" --> PG
    AW -- "7. execute tasks" --> PIPELINE
    PIPELINE -- "8. read/write" --> PG
    Airflow --- NET
    Storage --- NET

    style ENGINE fill:none,stroke:#888,stroke-width:2px,stroke-dasharray:5,color:#888
    style PIPELINE fill:#9C27B0,color:#fff
    linkStyle 0,1,2 stroke:#3b82f6,stroke-width:2px
    linkStyle 3,4 stroke:#f97316,stroke-width:2px
    linkStyle 5,6 stroke:#22c55e,stroke-width:2px
    linkStyle 7 stroke:#9C27B0,stroke-width:2px
    linkStyle 8 stroke:#14b8a6,stroke-width:2px
    linkStyle 9,10 stroke:#0d9488,stroke-width:2px
```

### Step 2b: DAG Execution — Skill Demand Stream

When `skill_demand_pipeline` DAG is triggered. Uses Spark + separate DB (port 5434).

```mermaid
flowchart TB
    classDef iface    fill:#dbeafe,stroke:#2563eb,color:#1e3a8a
    classDef cls      fill:#dcfce7,stroke:#16a34a,color:#14532d
    classDef abs      fill:#fef3c7,stroke:#d97706,color:#78350f
    classDef contract fill:#fce7f3,stroke:#db2777,color:#831843

    subgraph ENGINE["Docker Engine"]

    subgraph Airflow["📁 giljobi-airflow — already running"]
        AW["airflow-worker + Docker CLI"]:::cls
    end

    SOCK(["docker.sock"]):::iface
    UPSH_SPARK[["up.sh spark-sd"]]:::abs

    subgraph SparkCluster["📁 giljobi-spark-sd — created by DAG"]
        LIVY(["Livy :8998"]):::iface
        SM["spark-master :8080"]:::cls
        SW["spark-worker × N"]:::cls
    end

    SD_DB["📁 skill-demand-db<br/>PostgreSQL :5434"]

    NET{{"giljobi-network"}}

    end

    C_SPARK{{"«contract» ensure_spark<br/>Start on demand<br/>Stop after job"}}:::contract

    AW -- "1. up.sh spark-sd" --> UPSH_SPARK
    UPSH_SPARK -- "via" --> SOCK
    SOCK --> C_SPARK --> SM
    SM --> SW
    LIVY -- "2. submit jobs" --> SM
    LIVY -. "3. write results" .-> SD_DB

    Airflow --- NET
    SparkCluster --- NET

    style ENGINE fill:none,stroke:#888,stroke-width:2px,stroke-dasharray:5,color:#888
    style SD_DB fill:#BF360C,color:#fff
    linkStyle 0,1 stroke:#f97316,stroke-width:2px
    linkStyle 2,3 stroke:#9C27B0,stroke-width:2px
    linkStyle 4 stroke:#22c55e,stroke-width:2px
    linkStyle 5 stroke:#14b8a6,stroke-width:2px
    linkStyle 6 stroke:#ef4444,stroke-width:2px,stroke-dasharray:5
    linkStyle 7,8 stroke:#0d9488,stroke-width:2px
```

---

## Why This Layer Design?

### Problem

The original `docker-compose.yml` had **13 services in one file**. Starting anything meant starting everything — Spark clusters for a pipeline that only needed pandas, Celery workers when no tasks were queued.

### Solution: Modular Compose + DAG-Managed Lifecycle

Each compose file is an independent module. Streams pick only what they need:

| Module | Compose File | Lifecycle | Used by |
|--------|-------------|-----------|---------|
| **Airflow** | `docker-compose.airflow.yml` | Always on (manual start) | All streams |
| **Pipeline DB** | `docker-compose.postgres.yml` | ensure pattern (DAG starts, never stops) | market-trend |
| **Skill Demand DB** | `docker-compose.postgres-sd.yml` | ensure pattern (DAG starts, never stops) | skill-demand |
| **Spark + Livy** | `docker-compose.spark-sd.yml` | On-demand (DAG starts, DAG stops) | skill-demand only |

### What each stream uses

| Stream | Airflow | Pipeline DB | Spark |
|--------|:-------:|:-----------:|:-----:|
| **market-trend** | yes | yes (postgres) | no |
| **skill-demand** | yes | yes (postgres-sd) | yes (spark-sd) |
| *future stream* | yes | maybe | maybe |

### Benefits

- **No unnecessary services** — market-trend doesn't start Spark or skill-demand DB (saves 8GB+ RAM)
- **Independent scaling** — Airflow workers for I/O, Spark workers for compute
- **Failure isolation** — Spark crash doesn't take down Airflow or DB
- **Config-driven** — change replicas/memory in `.env`, no compose file edits
- **Stream independence** — each DAG declares what infrastructure it needs
- **Rebuild isolation** — rebuild Airflow image without touching Spark or DB

---

## Shared Infrastructure

### Airflow — Orchestration Layer

`docker-compose.airflow.yml` — Always on. The brain of the system.

```mermaid
flowchart TB
    classDef iface    fill:#dbeafe,stroke:#2563eb,color:#1e3a8a
    classDef cls      fill:#dcfce7,stroke:#16a34a,color:#14532d
    classDef abs      fill:#fef3c7,stroke:#d97706,color:#78350f
    classDef contract fill:#fce7f3,stroke:#db2777,color:#831843

    subgraph giljobi_airflow ["giljobi-airflow (6 services)"]
        ADB(["«db» airflow-db<br/>PostgreSQL 17 :5433"]):::iface
        REDIS(["«broker» giljobi-redis<br/>:6379"]):::iface

        C_HEALTH{{"«contract» Healthcheck<br/>DB + Redis must be ready<br/>before init runs"}}:::contract

        INIT["«one-shot» airflow-init<br/>DB migrate + admin user"]:::abs

        C_INIT{{"«contract» Init Complete<br/>DB migrated + admin created<br/>before services start"}}:::contract

        SCH["«scheduler»<br/>Reads DAGs<br/>Dispatches tasks"]:::cls
        WS["«webserver»<br/>DAG UI + REST API<br/>:8090"]:::cls
        AW["«worker»<br/>Executes tasks<br/>Has Docker CLI"]:::cls

        C_CELERY{{"«contract» Celery Queue<br/>Scheduler dispatches tasks<br/>Worker picks up via Redis"}}:::contract
        C_META{{"«contract» Metadata<br/>DAG state, task history<br/>stored in airflow-db"}}:::contract
    end

    DOCKER_SOCK(["«mount» /var/run/docker.sock<br/>Host Docker Socket"]):::iface

    ADB -- "1. ready?" --> C_HEALTH
    REDIS -- "1. ready?" --> C_HEALTH
    C_HEALTH -- "2. run" --> INIT
    INIT -- "3. done" --> C_INIT
    C_INIT -- "4. start" --> SCH
    C_INIT -- "4. start" --> WS
    C_INIT -- "4. start" --> AW
    SCH -- "5. dispatch" --> C_CELERY
    C_CELERY -- "6. pick up" --> AW
    C_CELERY -- "via" --> REDIS
    SCH -- "read/write" --> C_META
    WS -- "read" --> C_META
    C_META -- "store" --> ADB
    AW -- "mount" --> DOCKER_SOCK

    linkStyle 0,1 stroke:#22c55e,stroke-width:2px
    linkStyle 2 stroke:#22c55e,stroke-width:2px
    linkStyle 3 stroke:#a855f7,stroke-width:2px
    linkStyle 4,5,6 stroke:#a855f7,stroke-width:2px
    linkStyle 7,8 stroke:#3b82f6,stroke-width:2px
    linkStyle 9 stroke:#3b82f6,stroke-width:2px
    linkStyle 10,11 stroke:#a855f7,stroke-width:2px
    linkStyle 12 stroke:#a855f7,stroke-width:2px
    linkStyle 13 stroke:#f97316,stroke-width:2px
```

#### Services

| Service | Image | Port | Purpose |
|---------|-------|------|---------|
| `airflow-db` | postgres:17 | 5433 | Airflow metadata (DAG state, task history, users) |
| `giljobi-redis` | redis:latest | 6379 | Celery message broker |
| `airflow-webserver` | custom (Dockerfile) | 8090 | DAG UI, REST API |
| `airflow-scheduler` | custom (Dockerfile) | — | DAG parsing, task scheduling |
| `airflow-worker` | custom (Dockerfile) | — | Task execution (scalable replicas) |
| `airflow-init` | custom (Dockerfile) | — | One-shot: DB migration + admin user creation |

#### Airflow Worker Capabilities

The worker has special capabilities beyond standard Airflow:

- **Docker CLI installed** — can run `docker compose` commands
- **Docker socket mounted** — controls host Docker daemon
- **infra/ mounted** — access to `up.sh`, `down.sh`, compose files
- **pipelines/ mounted** — executes pipeline Python code directly
- **data/ mounted** — reads/writes raw and processed data

#### Volumes

| Mount | Container Path | Purpose |
|-------|---------------|---------|
| `../dags/` | `/opt/airflow/dags` | DAG files (one per stream) |
| `../pipelines/` | `/opt/airflow/pipelines` | All stream source code |
| `../data/` | `/opt/airflow/data` | Raw + processed data |
| `./` | `/opt/airflow/infra` | up.sh, down.sh, compose files |
| `./logs/` | `/opt/airflow/logs` | Airflow task logs |
| `/var/run/docker.sock` | `/var/run/docker.sock` | Host Docker control |

### Docker-in-Docker Architecture

Airflow worker is a container that controls the host's Docker Engine to create sibling containers (not nested).
Docker CLI inside the worker sends commands through the mounted `docker.sock` — a Unix socket file that Docker Engine listens on.

```mermaid
flowchart TB
    classDef iface    fill:#dbeafe,stroke:#2563eb,color:#1e3a8a
    classDef cls      fill:#dcfce7,stroke:#16a34a,color:#14532d
    classDef abs      fill:#fef3c7,stroke:#d97706,color:#78350f
    classDef contract fill:#fce7f3,stroke:#db2777,color:#831843

    subgraph ENGINE["Docker Engine"]

    subgraph AW ["Airflow Worker (container)"]
        DAG["«task» BashOperator<br/>ensure_db"]:::cls
        CLI["«tool» Docker CLI<br/>docker compose up"]:::cls
        UPSH[["«script» up.sh postgres"]]:::abs
        ENV_VAR["INFRA_HOST_PATH<br/>= C:/.../infra<br/>(env var from Step 1)"]:::abs
    end

    SOCK(["«socket» /var/run/docker.sock<br/>mounted from host"]):::iface

    PG(["«sibling container»<br/>market-trend-db<br/>PostgreSQL 17 :5432"]):::iface
    HOST_DIR["«host filesystem»<br/>C:/.../infra/init/<br/>01-market-trend.sql"]:::cls

    end

    C_SOCKET{{"«contract» Socket Mount<br/>Container → Host Docker<br/>via Unix socket"}}:::contract
    C_PATH{{"«contract» Host Path Resolve<br/>Volume mounts must use<br/>host absolute paths"}}:::contract

    DAG -- "1. execute" --> UPSH
    UPSH -- "2. read" --> ENV_VAR
    UPSH -- "3. call" --> CLI
    CLI -- "4. send command" --> C_SOCKET
    C_SOCKET --> SOCK
    SOCK -- "5. create container" --> PG
    ENV_VAR --> C_PATH
    C_PATH -- "6. mount" --> HOST_DIR
    HOST_DIR -- "7. init scripts" --> PG

    style ENGINE fill:none,stroke:#888,stroke-width:2px,stroke-dasharray:5,color:#888
    linkStyle 0 stroke:#3b82f6,stroke-width:2px
    linkStyle 1 stroke:#eab308,stroke-width:2px
    linkStyle 2 stroke:#3b82f6,stroke-width:2px
    linkStyle 3,4 stroke:#ec4899,stroke-width:2px
    linkStyle 5 stroke:#22c55e,stroke-width:2px
    linkStyle 6 stroke:#f97316,stroke-width:2px
    linkStyle 7 stroke:#f97316,stroke-width:2px
    linkStyle 8 stroke:#22c55e,stroke-width:2px
```

#### INFRA_HOST_PATH Flow

Volume mounts in Docker must use **host absolute paths**. When Airflow worker runs `docker compose up`, the Docker daemon resolves paths on the **host**, not inside the container.

```
1. User runs: ./infra/up.sh airflow
   └── up.sh sets: INFRA_HOST_PATH=$(pwd)  →  C:/Users/blitz/.../infra

2. Docker Compose passes INFRA_HOST_PATH to Airflow worker as env var

3. DAG triggers: up.sh postgres (inside Airflow worker)
   └── up.sh reads: INFRA_HOST_PATH (already set, skip resolve)
   └── compose uses: ${INFRA_HOST_PATH}/init  →  C:/Users/blitz/.../infra/init

4. Docker daemon mounts: C:/Users/blitz/.../infra/init → /docker-entrypoint-initdb.d/
   └── Host path exists → SQL files mounted correctly → tables created
```

### Configuration

All settings are externalized in `config/.env.{environment}`. Docker Compose reads `infra/.env` automatically.

#### Variables

| Category | Variable | Dev | Prod | Description |
|----------|----------|-----|------|-------------|
| **Pipeline DB** | `POSTGRES_USER` | postgres | postgres | DB username |
| | `POSTGRES_PASSWORD` | postgres | postgres | DB password |
| | `POSTGRES_DB` | giljobi | giljobi | DB name |
| | `PIPELINE_DB_CONN` | `...@market-trend-db:5432/giljobi` | `...@host.neon.tech/dbname` | Connection string for DAG tasks |
| | `MANAGE_PIPELINE_DB` | true | false | DAG starts local DB container |
| **Airflow DB** | `AIRFLOW_DB_USER` | airflow | airflow | Metadata DB username |
| | `AIRFLOW_DB_PASSWORD` | airflow | airflow | Metadata DB password |
| | `AIRFLOW_DB_NAME` | airflow | airflow | Metadata DB name |
| **Airflow** | `AIRFLOW_UID` | 50000 | 50000 | Container user ID |
| | `AIRFLOW_ADMIN_USER` | airflow | airflow | Web UI login |
| | `AIRFLOW_ADMIN_PASSWORD` | airflow | airflow | Web UI password |
| | `AIRFLOW_WORKER_REPLICAS` | 1 | 3 | Celery worker count |
| | `AIRFLOW_WORKER_MEMORY` | 2g | 4g | Memory limit per worker |
| **Spark** | `SPARK_WORKER_REPLICAS` | 1 | 5 | Spark worker count |
| | `SPARK_WORKER_CORES` | 1 | 4 | Cores per worker |
| | `SPARK_WORKER_MEMORY` | 2g | 8g | Memory per worker |
| | `MANAGE_SPARK` | true | true | DAG starts Spark cluster |

#### Environment Switching

```bash
# Development (default — auto-copied on first up.sh run)
cp config/.env.development .env

# Production
cp config/.env.production .env
```

### Scripts

#### up.sh — Start Modules

```mermaid
flowchart TB
    START["./infra/up.sh airflow --no-cache"]

    START --> CONFIG{".env exists?"}
    CONFIG -->|No| COPY["Copy config/.env.development → .env"]
    CONFIG -->|Yes| PARSE
    COPY --> PARSE

    PARSE["Parse arguments<br/>━━━━━━━━━━━<br/>Modules: airflow<br/>Flags: --no-cache"]

    PARSE --> NETWORK["docker network create<br/>giljobi-network<br/>(idempotent)"]

    NETWORK --> HOST_PATH{"INFRA_HOST_PATH<br/>set?"}
    HOST_PATH -->|No| RESOLVE["export INFRA_HOST_PATH=$(pwd)"]
    HOST_PATH -->|Yes| BUILD
    RESOLVE --> BUILD

    BUILD{"--no-cache?"}
    BUILD -->|Yes| REBUILD["docker compose build --no-cache<br/>for each module"]
    BUILD -->|No| EXEC
    REBUILD --> EXEC

    EXEC["For each module:<br/>docker compose<br/>-p {project-name}<br/>-f docker-compose.{module}.yml<br/>up -d --wait"]

    style START fill:#1565C0,color:#fff
    style NETWORK fill:#2E7D32,color:#fff
    style EXEC fill:#4CAF50,color:#fff
```

#### Module → Project Name Mapping

| Module | Project Name | Compose File | Docker Desktop Group |
|--------|-------------|-------------|---------------------|
| `postgres` | `market-trend-db` | `docker-compose.postgres.yml` | 📁 market-trend-db |
| `postgres-sd` | `skill-demand-db` | `docker-compose.postgres-sd.yml` | 📁 skill-demand-db |
| `airflow` | `giljobi-airflow` | `docker-compose.airflow.yml` | 📁 giljobi-airflow |
| `spark-sd` | `giljobi-spark-sd` | `docker-compose.spark-sd.yml` | 📁 giljobi-spark-sd |

#### down.sh — Stop Modules

```bash
./infra/down.sh                    # Stop all modules
./infra/down.sh airflow            # Stop Airflow only
./infra/down.sh postgres -v        # Stop DB + delete volume (full reset)
./infra/down.sh -v                 # Stop all + delete all volumes
```

---

## Stream SPECs

Each stream has its own SPEC with pipeline architecture, DAG flow, and data details:

| Stream | SPEC | Status |
|--------|------|--------|
| Market Trend | [pipelines/market-trend/SPEC.md](../pipelines/market-trend/SPEC.md) | Completed |
| Skill Demand | [pipelines/skill-demand/SPEC.md](../pipelines/skill-demand/SPEC.md) | E2E verified on sample |
