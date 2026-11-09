# Analytics — a Postgres endpoint for your data lake

Run a **modern data lake** on your own Kubernetes cluster, then query it like it's just... Postgres.

* **[RustFS](https://rustfs.com) v1.0.4** — the freshly released, S3-compatible object store written in Rust. Deployed distributed (4 pods × 1 drive, Longhorn PVCs) and hosting an **Apache Iceberg REST catalog** over your data.
* **DuckDB 2.0.0** — in-process analytical engine, `:memory:`, with the `iceberg` and `httpfs` extensions. It `ATTACH`es the RustFS catalog and reads Parquet data straight from object storage. No state to babysit.
* **DuckFlight** — a DuckDB community extension that exposes the **PostgreSQL wire protocol** (`:5433`) and **Arrow Flight SQL** (`:31337`), with TLS and PBKDF2-hashed credentials generated per boot.

The result: any Postgres client you already have — `psql`, DBeaver, Metabase, your app's ORM — connects and runs analytical SQL over the lake. No warehouse to size, no cluster to manage, no catalog to install separately.

* **Metabase v0.65.3.6** — shipped in-repo as the BI layer, talking to DuckFlight over the Postgres wire like any other client (see [Metabase](#metabase)).

## The pitch in one query

```bash
psql -h duckdb -p 5433 -U analyst
```

```sql
SELECT *
FROM duckflight_pg_serve('0.0.0.0:5433', '/runtime/duckflight.toml');
-- server up. now, from anywhere:

psql -h duckdb -p 5433 -U analyst -c "
  SELECT event, count(*)
  FROM   datalake.metrics.events
  WHERE  day >= today() - 7
  GROUP  BY event;"
```

That's a distributed Iceberg table on object storage, being scanned by DuckDB's vectorized engine, over a connection that your Postgres driver already speaks.

## Architecture

### Bird's-eye view

```mermaid
flowchart LR
    PG["Postgres clients<br/>psql · DBeaver · Metabase · ORMs"]
    FS["Arrow Flight SQL clients<br/>Spark · pandas · JDBC"]

    subgraph ANALYTICS["namespace: analytics"]
        SVC["Service/duckdb<br/>5433 postgres · 31337 flight<br/>Tailscale: opt-in"]
        DUCK["Deployment/duckdb<br/>DuckDB 2.0.0 :memory:<br/>iceberg · httpfs · duckflight"]
        HPA["HPA/duckdb<br/>1-3 replicas · cpu 65%"]
        VPA["VPA/duckdb<br/>memory only · 2-6Gi"]
        SETTINGS["ConfigMap/duckdb-settings<br/>runtime facts: endpoint, region, ports"]
        CFG["ConfigMap/duckdb-config<br/>generate-config.sh + init.sql"]
        AUTH["Secret/duckflight-auth<br/>username · password"]
        CREDS["Secret/rustfs-credentials<br/>RUSTFS_ACCESS_KEY · RUSTFS_SECRET_KEY"]
    end

    subgraph RUSTFS["namespace: rustfs"]
        RSVC["Service/rustfs-svc<br/>9000 S3 + Iceberg REST<br/>Tailscale: opt-in"]
        STS["StatefulSet/rustfs<br/>4 pods · 10Gi longhorn each"]
    end

    PG --> SVC
    MB["Deployment/metabase<br/>v0.65.3.6 · Tailscale: metabase"] -- "pg wire, dbname=datalake" --> SVC
    MB --> MBDB[("StatefulSet/metabase-db<br/>Postgres 18 · longhorn 5Gi")]
    FS --> SVC
    SVC --> DUCK
    HPA -. scales .- DUCK
    VPA -. resizes memory requests .- DUCK
    SETTINGS -. envFrom .-> DUCK
    CFG -. init renders toml + TLS .-> DUCK
    AUTH -. env .-> DUCK
    CREDS -. env .-> DUCK
    DUCK == "S3 API + Iceberg REST SIGV4" ==> RSVC
    MAINT["CronJob/iceberg-maintenance-daily<br/>Spark local: compaction ·<br/>manifests · expiry · orphans"]
    CREDS -. env .-> MAINT
    SETTINGS -. env .-> MAINT
    MAINT == "Iceberg REST SIGV4 + S3 FileIO" ==> RSVC
    RSVC --> STS
    subgraph POLARIS["namespace: polaris"]
        PL["Deployment/polaris 1.8.0<br/>Iceberg REST :8181/api/catalog<br/>catalog lakehouse · OAuth2 principals"]
        PLDB[("StatefulSet/polaris-db<br/>Postgres 18 · longhorn 5Gi")]
    end
    DUCK == "Iceberg REST, OAuth2 (principal duckdb)" ==> PL
    PL == "S3 (polaris user)" ==> RSVC
    PL --> PLDB
```

* DuckDB pods are **disposable** — the only state is in the lake. The HPA scales `duckdb` 1→3 on CPU pressure; the VPA owns memory sizing (2–6Gi, `InPlaceOrRecreate` — request raises resize the pod in place, no restart), and each pod re-derives its `memory_limit` from the request at start.
* Only **Metabase** (`http://metabase`) and the **Polaris Console** (`http://polaris-console`) are on the **Tailscale** tailnet by default. Metabase reaches DuckDB in-cluster, as do all other consumers. `duckdb` and `rustfs` join the tailnet only when a deploy asks for it — see [Tailscale exposure](#tailscale-exposure).

### What happens at pod start

```mermaid
sequenceDiagram
    autonumber
    participant INIT as init container<br/>generate-duckflight-config
    participant DUCK as duckdb container
    participant RFS as RustFS<br/>rustfs-svc :9000

    Note over INIT: env sources<br/>ConfigMap duckdb-settings<br/>Secret duckflight-auth<br/>Secret rustfs-credentials
    INIT->>INIT: render /runtime/duckflight.toml<br/>PBKDF2 hash + TLS cert<br/>SANs from service.namespace
    INIT->>DUCK: /runtime ready
    DUCK->>DUCK: INSTALL/LOAD iceberg · httpfs · duckflight
    DUCK->>DUCK: SET memory_limit = 85% of request<br/>(Downward API env → getenv())
    DUCK->>DUCK: CREATE SECRET rustfs_s3<br/>all values via getenv()
    DUCK->>RFS: ATTACH datalake<br/>Iceberg REST endpoint from env
    Note over INIT,DUCK: lakehouse: the init container signs in to Polaris<br/>and probes the catalog (≤90s), then writes the ATTACH<br/>or a no-op, which init.sql .reads — a Polaris outage<br/>never stops duckdb from starting
    DUCK->>DUCK: duckflight_pg_serve 0.0.0.0:5433
    DUCK->>DUCK: duckflight_flight_serve 0.0.0.0:31337
    Note over DUCK: Postgres wire + FlightSQL live,<br/>lake is one query away
```

### Keeping DuckDB aligned with RustFS

```mermaid
flowchart TD
    A["workflow: rustfs applied"] --> B["scripts/sync-duckdb-rustfs.sh"]
    B --> C{"read live<br/>Service/rustfs-svc"}
    C --> D["derive RUSTFS_ENDPOINT_HOST/PORT"]
    D --> E{"ConfigMap/duckdb-settings<br/>already matches?"}
    E -- yes --> F["exit 0 - no restart"]
    E -- no --> G["patch ConfigMap<br/>in namespace analytics"]
    G --> H["kubectl rollout restart<br/>deployment/duckdb"]
    H --> I["init.sql re-reads env<br/>via getenv()"]
```

### Facts at a glance

| Aspect | Value |
|---|---|
| Namespaces | `analytics` (duckdb, metabase) · `rustfs` (object store) · `polaris` (Iceberg REST catalog) |
| DuckDB endpoints | `5433` Postgres wire · `31337` Arrow Flight SQL |
| RustFS endpoint | `rustfs-svc.rustfs.svc.cluster.local:9000` (S3 + Iceberg REST `/iceberg`) |
| Default bucket | `datalake` — created and enabled as an S3 table bucket by `Job/rustfs-bootstrap` on every rustfs/all apply (RustFS built-in catalog) |
| Lakehouse bucket | `lakehouse` — plain bucket, catalog = **Apache Polaris** 1.8.0 (`http://polaris.polaris.svc.cluster.local:8181/api/catalog`, catalog `lakehouse`, OAuth2 client credentials) |
| Object storage | RustFS chart v1.0.4, distributed mode 4 pods × 1 drive, Longhorn 10Gi per pod |
| Scaling | duckdb: HPA 1–3 replicas (CPU 65%) + VPA memory 2–6Gi (InPlaceOrRecreate) — `memory_limit` = 85% × request via the Downward API |
| Exposure | Tailscale: `metabase` and `polaris-console` always; `duckdb` / `rustfs` opt-in per deploy (`tailscale_expose`) |
| Auth | DuckFlight PBKDF2-hashed creds + TLS, per-boot cert with `<service>.<ns>` SANs |
| Credentials | Single GitHub Secret pair, stamped by CI into both namespaces |
| Iceberg maintenance | CronJob `iceberg-maintenance-daily` in `analytics` at 02:00 (compact → manifests → expire snapshots → orphan cleanup) · Spark local mode · image `ghcr.io/daun-gatal/analytics/iceberg-maintenance` (public, built by CI) |
| BI | `Deployment/metabase` (v0.65.3.6, 1 replica) + `StatefulSet/metabase-db` (Postgres 18.6, Longhorn 5Gi) — the only stateful piece in `analytics`; `Secret/metabase-secrets` generated once by CI |
| Data generator | `Deployment/generator-stream` (always-on: Wikimedia EventStreams SSE + Bluesky Jetstream WS → `datalake.events.*`, 60s/100-row buffered appends) + `CronJob/generator-batch` hourly at :15 (GH Archive + Wikimedia pageview dumps → `datalake.web.*`, overwrite-by-hour) · image `ghcr.io/daun-gatal/analytics/generator` (public, built by CI) |

## Structure

```
analytics/                     # repo root for manifests (this dir)
├── namespace.yaml             # Namespace/analytics
├── kustomization.yaml         # root — order: namespace -> duckdb -> rustfs -> polaris -> maintenance -> generator -> metabase
├── duckdb/                    # DuckDB + DuckFlight (Postgres/FlightSQL endpoints)
│   ├── kustomization.yaml
│   ├── config-settings.yaml         # duckdb-settings ConfigMap (runtime facts)
│   ├── configmap.yaml               # duckdb-config (generate-config.sh + init.sql, getenv-driven)
│   ├── deployment.yaml              # duckdb/duckdb:2.0.0, :memory:, init renders config
│   ├── service.yaml                 # 5433 postgres, 31337 flight (tailnet: opt-in)
│   ├── hpa.yaml                     # 1..3 replicas, cpu 65% (memory → VPA)
│   └── vpa.yaml                     # VPA: memory 2–6Gi, InPlaceOrRecreate — request feeds the Downward API env
├── rustfs/                    # RustFS object store (chart v1.0.4, StatefulSet x4)
│   ├── kustomization.yaml           # Namespace/rustfs + helmCharts (repo charts.rustfs.com)
│   ├── namespace.yaml               # Namespace/rustfs
│   ├── helm.yaml                    # chart values (distributed 4x1, longhorn)
│   ├── bootstrap-config.yaml        # rustfs-bootstrap-config: bootstrap.sh + RUSTFS_ENDPOINT/BUCKET_NAME
│   ├── iam-config.yaml              # rustfs-iam-config: RustFS policies + iam.sh (users bezisync, polaris)
│   └── bootstrap-job.yaml           # Job/rustfs-bootstrap — buckets, BeziSync/Polaris users, per apply
├── polaris/                   # Apache Polaris Iceberg REST catalog (chart 1.8.0)
│   ├── kustomization.yaml           # Namespace/polaris + helmCharts (repo downloads.apache.org/polaris)
│   ├── namespace.yaml               # Namespace/polaris
│   ├── helm.yaml                    # chart values: relational-jdbc, token key, RustFS keys, schema + realm bootstrap init containers
│   ├── postgres.yaml                # polaris-db: headless Service + Postgres 18 StatefulSet (metastore)
│   ├── setup-config.yaml            # polaris-setup-config: setup-polaris.py + knobs
│   ├── setup-job.yaml               # Job/polaris-setup — catalog, roles, principals, lakehouse destination, per apply
│   └── console.yaml                 # Polaris Console (CI-built image) + /api/ forwarding to Polaris, tailnet `polaris-console`
├── maintenance/               # Iceberg table maintenance (Spark local-mode CronJobs)
│   ├── kustomization.yaml           # pins the CI-built ghcr.io image
│   ├── configmap.yaml               # maintenance-config: maintenance.py + MAINT_* tunables
│   ├── cronjob.yaml                 # daily: compact / manifests / expire snapshots
│   └── image/
│       └── Dockerfile               # apache/spark + iceberg runtime + aws bundle
├── generator/                   # streaming + batch data generator (PyIceberg writers)
│   ├── kustomization.yaml           # pins the CI-built ghcr.io image
│   ├── generator-settings.yaml      # generator-settings: GENERATOR_* tunables
│   ├── streaming.py                 # EventStreams SSE + Jetstream WS -> events.*
│   ├── batch.py                     # GH Archive + pageview dumps -> web.*
│   ├── Dockerfile                   # python slim + pyiceberg + pyarrow
│   ├── stream/
│   │   └── deployment.yaml          # Deployment/generator-stream (1 replica)
│   └── batch/
│       └── cronjob.yaml             # CronJob/generator-batch (hourly :15)
├── metabase/                  # Metabase BI over DuckFlight
│   ├── kustomization.yaml
│   ├── postgres.yaml                # metabase-db: headless Service + Postgres 18 StatefulSet (app DB)
│   ├── deployment.yaml              # metabase/metabase:v0.65.3.6, 1 replica, Recreate
│   ├── service.yaml                 # :80 -> 3000, tailscale expose `metabase`
│   ├── register-config.yaml         # register-duckdb.py + knobs
│   └── register-job.yaml            # Job/metabase-register — add DuckDB as `Lake`, per apply
├── tailscale/                 # opt-in tailnet exposure (kustomize Components)
│   ├── duckdb/kustomization.yaml    # annotates Service/duckdb → hostname `duckdb`
│   └── rustfs/kustomization.yaml    # annotates Service/rustfs-svc → hostname `rustfs`
├── scripts/
│   ├── build-manifests.sh     # render a module (+ tailscale components) — used by CI
│   └── sync-duckdb-rustfs.sh  # derive duckdb connection facts from deployed rustfs
└── .github/workflows/
    └── deploy.yaml            # module/action dispatcher (dispatch + call)
```

## Parameterization

Everything that can be parameterized is, via env / ConfigMap / Secret:

| Kind | Object | What it carries |
|---|---|---|
| ConfigMap | `analytics/duckdb-settings` | `DUCKDB_SERVICE_NAME`, `RUSTFS_PROTOCOL`, `RUSTFS_ENDPOINT_HOST/PORT`, `RUSTFS_REGION`, `DUCKFLIGHT_PG_PORT`, `DUCKFLIGHT_FLIGHT_PORT` |
| Secret | `analytics/duckflight-auth` | DuckFlight `username`/`password` (created by CI from GitHub Secrets) |
| Secret | `analytics` + `rustfs` `rustfs-credentials` | `RUSTFS_ACCESS_KEY`/`RUSTFS_SECRET_KEY` — same keys in both namespaces so chart and duckdb always match (created by CI from one GitHub Secret pair) |
| ConfigMap | `analytics/maintenance-config` | `maintenance.py` (the maintenance script) + tunables `MAINT_TARGET_FILE_SIZE`, `MAINT_SNAPSHOT_MAX_AGE`, `MAINT_RETAIN_LAST`, `MAINT_ORPHAN_MIN_AGE` — human-friendly units (`512MB`, `7d`, `72h`) parsed by the script |

Flow at pod start: the `generate-duckflight-config` init container renders `/runtime/duckflight.toml` (DuckFlight auth + TLS; SANs derived from `<service>.<namespace>`). The duckdb container runs `-init /etc/duckdb/init.sql`, which reads **all runtime facts from the container env via `getenv()`** — endpoint, region, protocol (which derives `USE_SSL`), ports, and S3 credentials. No SQL is rendered at startup; `init.sql` is the ConfigMap's plain SQL.

One value is computed rather than configured: after the LOADs, `init.sql` caps the buffer manager at **85% of the container's memory request** (`DUCKDB_MEMORY_REQUEST_BYTES`, exposed via the Downward API in `deployment.yaml`). `SET` accepts runtime functions (DuckDB ≥ 0.10.0), so the cap re-derives on every pod start. The VPA raises requests in place (no restart), so a raise takes effect for scheduling immediately while the buffer cap re-syncs at the next pod start (fallback: 85% of 2Gi if the env is empty — `getenv()` returns `''`, not NULL, for missing vars).

One deliberate literal: `ATTACH 'datalake' AS datalake` — DuckDB's grammar does not accept expressions for the catalog path/alias, so the catalog identifier is a SQL literal while everything else (endpoint, region, protocol, credentials) is env-driven.

## RustFS → DuckDB alignment

`scripts/sync-duckdb-rustfs.sh` is the alignment loop — rustfs is the source of truth:

1. Reads the **live** `Service/rustfs-svc` in ns `rustfs`
2. Derives `RUSTFS_ENDPOINT_HOST` (`<svc>.<ns>.svc.<cluster-domain>`) and `RUSTFS_ENDPOINT_PORT` (Service port named `endpoint`)
3. Patches ConfigMap `analytics/duckdb-settings` when the values differ
4. `kubectl rollout restart deployment/duckdb -n analytics` so `init.sql` re-reads

The script is idempotent — if the configmap already matches, it exits 0 with no restart. Run manually:

```bash
bash scripts/sync-duckdb-rustfs.sh
```

## Default bucket bootstrap

`Job/rustfs-bootstrap` (ns `rustfs`) makes the default bucket exist and be query-ready as part of every deploy — no console clicks, no manual `aws`/`mc` steps:

1. waits for `Service/rustfs-svc` readiness (`/health/ready`, the same path the chart's probes use — up to 5 min, so the 4-pod distributed set can converge)
2. creates bucket **`datalake`** when missing (ordinary S3 `CreateBucket`; `409 BucketAlreadyOwnedByYou` counts as success)
3. enables it as an **S3 table bucket** — `PUT /iceberg/v1/buckets/datalake`, the built-in Iceberg REST catalog action ([RustFS S3 Tables guide](https://docs.rustfs.com/en/administration/data/s3-tables))
4. reads the state back and asserts `enabled: true` + `warehouse == datalake` before exiting 0

* **All requests are SigV4-signed** with `curl --aws-sigv4` (image `curlimages/curl`, curl ≥ 7.76) using the existing `rustfs-credentials` Secret — no new secrets, no SDK image.
* **Idempotent** — every step verifies-or-mutates, so re-runs against a bucket that already holds live table data are safe; enabling an existing bucket is supported.
* **Every node serves the catalog**: after enabling, it checks the catalog on each RustFS pod directly (`rustfs-<i>.rustfs-headless`, `RUSTFS_REPLICAS` = `replicaCount`) and **re-enables the table bucket on any pod that doesn't serve it yet**. Each RustFS node caches bucket metadata, and a change reaches peers by an internode reload RPC (full reload only every 15 min). An enable right after a fresh start can lose that RPC, so 2 of 4 nodes kept answering `400 bucket datalake is not table-enabled` while the others said enabled. Re-sending the enable to the lagging node is the automated version of the old manual "enable S3 Tables again" fix.
* **Re-runs per deploy** — CI drops the previous Job before every `rustfs`/`all` apply (Job specs are immutable) and `ttlSecondsAfterFinished: 3600` GCs the rest, so each apply re-runs and re-verifies it. The wait polls both `complete` and `failed` (like the `trigger` action) for up to 10 min and fails with the Job's logs if the state can't be ensured.
* **Tunables** — `RUSTFS_ENDPOINT`, `BUCKET_NAME`, `AWS_DEFAULT_REGION` in `rustfs/bootstrap-config.yaml`; the signing region must match `config.rustfs.region` in `rustfs/helm.yaml`.

Re-run manually (e.g. after wiping the Longhorn PVCs):

```bash
kubectl delete job/rustfs-bootstrap -n rustfs --ignore-not-found
kubectl kustomize --enable-helm rustfs/ | kubectl apply -f -
kubectl logs -f job/rustfs-bootstrap -n rustfs
```

DuckDB's `ATTACH 'datalake'` and the generators' `create_namespace_if_not_exists` need nothing else — Iceberg namespaces and tables self-heal on first write.

## Polaris catalog (`lakehouse`)

The `polaris/` module runs [Apache Polaris](https://polaris.apache.org) **1.8.0** (official Helm chart) in its own namespace **`polaris`**, with its own Postgres (`polaris-db`, Longhorn 5Gi, `relational-jdbc` persistence). It serves the Iceberg catalog **`lakehouse`** on the plain bucket **`s3://lakehouse`**. RustFS is just object storage there. The older `datalake` table bucket keeps RustFS's built-in catalog, untouched.

* **Catalog URI:** `http://polaris.polaris.svc.cluster.local:8181/api/catalog`, warehouse `lakehouse`. Token URL `…/api/catalog/v1/oauth/tokens`, scope `PRINCIPAL_ROLE:ALL`.
* **Auth:** Polaris's own principals (OAuth2 client credentials) and role grants, set up by `Job/polaris-setup`:

  | Principal | Principal role → catalog role | Privileges | Secret |
  |---|---|---|---|
  | `duckdb` | `lakehouse_reader` → `lakehouse.reader` | list + read namespaces, tables, views | `polaris-duckdb` (also copied to ns `analytics`) |
  | `bezisync` | `lakehouse_writer` → `lakehouse.writer` | reader + create namespaces/tables, commit; **no drop** | `polaris-bezisync` |
  | `admin` | `service_admin` (built-in) | everything — the **console login** | `polaris-admin` |
  | `root` | `service_admin` (realm bootstrap) | everything — used only by the setup Job | `polaris-root` |

  Add a client by adding a `PRINCIPALS` entry in `polaris/setup-config.yaml` plus its Secret in `deploy.yaml`.
* **Storage:** RustFS has no STS, so the catalog is created with `stsUnavailable`. Polaris vends **no** credentials (a vending request is refused with HTTP 400, and `loadTable` returns only the endpoint settings). Polaris writes table metadata with its own RustFS user `polaris` (policy `polaris-warehouse`, objects on `lakehouse` only). Every client reads and writes data files with its own scoped RustFS user and must not request credential vending: DuckDB uses `ACCESS_DELEGATION_MODE 'none'`, BeziSync uses `storage: s3`, and PyIceberg needs `header.X-Iceberg-Access-Delegation: ""`.
* **In DuckDB / Metabase:** duckdb attaches it as catalog **`lakehouse`** (`POLARIS_URL` / `POLARIS_CATALOG` in `duckdb-settings`, credentials from `Secret/polaris-duckdb`). The init container signs in and probes `…/v1/config?warehouse=lakehouse` for up to 90 s, then writes either the `ATTACH` or a no-op that `init.sql` `.read`s, so a Polaris outage never keeps duckdb (and `datalake`) down. The client secret stays in env: the `ATTACH` reads it with `getenv()`. DuckDB renews its token by itself when it expires. After `polaris-setup`, CI restarts duckdb once unless every duckdb pod's init log says `Polaris catalog … will be attached`. Query it over DuckFlight with **dbname `lakehouse`**, i.e. Metabase database **`Lakehouse`**.
* **Why:** a real REST catalog gives upserts and table format v3 (BeziSync's RustFS preset is append-only, v2), avoids the built-in catalog's node-sync issue above, and Polaris adds per-client permissions without an extra identity provider or authorization service.
* **Bootstrap:** two init containers on the Polaris pod run on every start, before the server: `CREATE SCHEMA IF NOT EXISTS polaris_schema`, then the admin tool's realm `bootstrap` (prints `already bootstrapped; skipping` after the first time). Polaris never starts against an un-bootstrapped database.
* **Setup:** `Job/polaris-setup` (every polaris/all apply) creates or updates the catalog, its roles and grants, and the principals. It pins each principal's credentials to its Secret, and verifies every principal end to end. The bucket and Polaris's RustFS user come from `Job/rustfs-bootstrap`.
* **Secrets:** CI generates once, never rotated: `polaris-db` (must keep matching the volume), `polaris-root` (fixed at the realm's first bootstrap), `polaris-token-key` (signs access tokens; a fixed key keeps tokens valid across restarts), `polaris-duckdb`, `polaris-bezisync`, and `rustfs-polaris` (ns `rustfs`, copied to `polaris`).
* **Console:** [Polaris Console](https://polaris.apache.org/tools/polaris-console/) at **`http://polaris-console`** (tailnet). Log in as principal **`admin`**:
  ```bash
  kubectl get secret polaris-admin -n polaris -o jsonpath='{.data.client-id}' | base64 -d      # admin
  kubectl get secret polaris-admin -n polaris -o jsonpath='{.data.client-secret}' | base64 -d
  ```
  * **Image:** upstream (`apache/polaris-tools`, `console/`) has never released one. CI builds it from the commit pinned as the `polaris-console` image tag in `polaris/kustomization.yaml`, the only place the pin lives. It's pushed as `ghcr.io/daun-gatal/analytics/polaris-console:<commit>` and built only when that tag isn't on GHCR yet. To update, set a newer polaris-tools commit (full sha) there. The first push creates the package **private**: flip it to public once, like the other two images. The console is unreleased upstream `main`, so expect rough edges.
  * **One origin:** the app runs in the browser and calls the Polaris API itself. The console's nginx forwards `/api/` to Polaris in-cluster (`polaris-console-proxy` ConfigMap), and the app is pointed at its own URL. The browser never talks to Polaris directly: no CORS, and Polaris stays off the tailnet. The forwarded API still needs a principal's OAuth2 token.
  * Anyone on the tailnet reaches the login page; only `admin`'s credentials get further.
* Validated in a scratch namespace (2026-11-10, Polaris 1.8.0 + RustFS 1.0.4): rerunnable bootstrap and setup; PyIceberg 0.10.0 as `bezisync` create, append and add-column, with drop table/namespace denied; `duckdb` read with create denied; no credentials vended; DuckDB 2.0.0 `ATTACH` with token renewal across expiry; BeziSync draft check of the Polaris destination (same config the lab registers as `analytics-lakehouse`): Ready.

## BeziSync access

[BeziSync](https://github.com/daun-gatal/bezisync) (CDC on Debezium Server) writes into both catalogs. This repo gives it **access** and nothing else. The BeziSync **destinations** themselves are registered by [bezisync-lab](https://github.com/daun-gatal/bezisync-lab) in its Playground project, with templates **Shop → Analytics lake** and **Shop → Analytics lakehouse**. Its `playground.sh apply` reads the credentials below from the cluster and skips the lake's destinations when this stack isn't deployed. Deploy this repo first, then (re-)run the lab's apply.

| BeziSync destination (bezisync-lab) | Catalog | Access from this repo | Secret |
|---|---|---|---|
| `analytics-lake` | RustFS built-in, `datalake` table bucket (append-only change log, v2) | RustFS user `bezisync` (policy `bezisync-iceberg`) | `rustfs-bezisync` (ns `rustfs`) |
| `analytics-lakehouse` | Polaris, catalog `lakehouse` (upserts / deletes, v2 or v3) | Polaris principal `bezisync` (`lakehouse.writer`) for the catalog, RustFS user `bezisync` for data files | `polaris-bezisync` (ns `polaris`) + `rustfs-bezisync` |

`Job/rustfs-bootstrap`'s **`iam`** container (`rustfs/rc` image, `rustfs/iam-config.yaml`) upserts the RustFS policies and users on every apply. `bezisync` gets Iceberg catalog actions and objects on `datalake`, plus plain object access on `lakehouse`, with no drop/rename and no other bucket. `polaris` gets objects on `lakehouse` only. CI generates their secrets once in-cluster. `Job/polaris-setup` keeps the Polaris principal `bezisync` pinned to `polaris-bezisync`.

## Iceberg table maintenance

`maintenance/` runs Apache Iceberg table maintenance as two Kubernetes CronJobs in ns `analytics`, driven by a **Spark local-mode** engine in a custom image built by CI. DuckDB can't do this — its `iceberg` extension is read-only — so the maintenance job talks to the same RustFS REST catalog (SigV4) and S3 endpoint directly, using the *same* env facts (`duckdb-settings` ConfigMap + `rustfs-credentials` Secret). No new secrets, no new services.

| CronJob | Schedule (UTC) | What it runs, per table |
|---|---|---|
| `iceberg-maintenance-daily` | `0 2 * * *` | `rewrite_data_files` (bin-pack to target size) → `rewrite_manifests` → `expire_snapshots` → `remove_orphan_files` (72h grace window) |

* **Discovery is dynamic** — the script lists all namespaces/tables via `SHOW NAMESPACES/TABLES` on each run; no table list to maintain
* **Per-table isolation** — one failing table doesn't block the others; the job exits non-zero if anything failed
* **Configurable** — edit `schedule:` in `maintenance/cronjob.yaml`; operation tunables are env keys in `maintenance/configmap.yaml` (`MAINT_TARGET_FILE_SIZE`, `MAINT_SNAPSHOT_MAX_AGE`, `MAINT_RETAIN_LAST`, `MAINT_ORPHAN_MIN_AGE`)
* **Light footprint** — `requests 500m/1Gi`, `limits 2 CPU/2Gi`, `local[2]`, runs at 02:00 when the cluster is idle; `concurrencyPolicy: Forbid`

Image: `ghcr.io/daun-gatal/analytics/iceberg-maintenance` — `apache/spark:4.0.4-scala2.13-java17-python3-ubuntu` + `iceberg-spark-runtime-4.0_2.13` + `iceberg-aws-bundle` (both 1.11.0, pinned via ARGs in `maintenance/image/Dockerfile`). The maintenance script itself lives in the ConfigMap, so tuning it never needs an image rebuild.

> **One-time setup:** the CI `build-image` job publishes the package on first push. GitHub creates new GHCR packages as **private** — flip it to public once (GitHub → Packages → `iceberg-maintenance` → Package settings → Change visibility). After that the cluster pulls it without a pull secret.

Trigger a run manually (e.g. after a big backfill):

```bash
kubectl create job --from=cronjob/iceberg-maintenance-daily maintenance-manual -n analytics
kubectl logs -f job/maintenance-manual -n analytics
```

## Data generator

`generator/` feeds the lake with real internet data so the Iceberg tables aren't empty. Two distinct pipelines, one small Python image (`ghcr.io/daun-gatal/analytics/generator`, PyIceberg writing through the same RustFS REST catalog with SigV4 as the maintenance job — reuses `duckdb-settings` + `rustfs-credentials`, no new secrets):

| Pipeline | Workload | Internet source | Iceberg tables |
|---|---|---|---|
| **Streaming** | `Deployment/generator-stream` (1 replica, Recreate) | Wikimedia `EventStreams` `/v2/stream/recentchange` (SSE) · Bluesky `Jetstream` `wss://jetstream.us-east.bsky.network/subscribe` (WebSocket) | `datalake.events.wikipedia_edits` · `datalake.events.bluesky_posts` |
| **Batch** | `CronJob/generator-batch` hourly at :15 | GH Archive `data.gharchive.org/{Y-M-D-H}.json.gz` · Wikimedia pageview dumps `dumps.wikimedia.org/other/pageviews/{Y}/{Y-M}/pageviews-*.gz` | `datalake.web.github_events` · `datalake.web.wikimedia_pageviews` |

* **Streaming appends are buffered** — flush at ≥100 rows or 60s, so the firehose produces a few real commits per minute, not a snapshot per event; the daily maintenance bin-packs whatever accumulates.
* **Batch is overwrite-by-hour** — each run fetches the previous complete hour and *replaces* that slice (`overwrite` with a `ts` predicate), so re-running a CronJob never duplicates rows. Pageview dumps publish ~2–4h behind, so the script walks back up to `GENERATOR_BATCH_BACKTRACK_HOURS` to find the newest published file.
* **Both sources resilient** — SSE/WebSocket auto-reconnect with exponential backoff; the pod's liveness probe checks a heartbeat the main loop stamps every 5s.
* **Namespaces and tables are auto-created** on first write (`create_namespace_if_not_exists` for `events` / `web`, then `create_table_if_not_exists`, partitioned by `day(ts)`) — no manual DDL, and re-deploys on an empty catalog self-heal; Bluesky rows keep typed columns plus the raw event JSON in a `raw` column for schema-proofing.
* **Tunables** live in the `generator-settings` ConfigMap (`GENERATOR_FLUSH_MAX_ROWS`, `GENERATOR_FLUSH_INTERVAL_S`, `GENERATOR_BATCH_MAX_ROWS`, `GENERATOR_BATCH_BACKTRACK_HOURS`, …).
* **Offline proof without RustFS** — two levels of it:
  * `GENERATOR_DRY_RUN=true`: fetches the real sources but writes plain Parquet to `DRY_RUN_DIR` instead of touching any catalog (fetch/parse/shape smoke test).
  * `GENERATOR_CATALOG_TYPE=sql GENERATOR_CATALOG_URI=sqlite:… GENERATOR_CATALOG_WAREHOUSE=file://…`: runs the **real Iceberg write path** — namespace creation, table creation, appends and overwrite-by-hour commits — against a local SQLite catalog with a file warehouse. This is how the module was validated before any RustFS integration.

> **One-time setup:** like the maintenance image, the first CI push creates the `generator` GHCR package **private** — flip it to public once (GitHub → Packages → `generator` → visibility).

Trigger a manual batch run:

```bash
kubectl create job --from=cronjob/generator-batch generator-manual -n analytics
kubectl logs -f job/generator-manual -n analytics
```

## Metabase

`metabase/` runs Metabase OSS as the BI layer. It is just another Postgres client of DuckFlight — no DuckDB driver plugin, no direct lake access, no RustFS credentials.

* **App DB:** a single-replica Postgres 18 StatefulSet (`metabase-db`, Longhorn 5Gi) stores users, questions, dashboards and the encrypted DuckFlight login. `Secret/metabase-secrets` (`db-password`, `encryption-key`) is generated by CI on the **first** deploy and never re-applied — rotating `encryption-key` would make stored connections undecryptable.
* **Access:** `http://metabase` on the tailnet. The first visit runs Metabase's setup wizard (admin account).

**Auto-setup + registration (no API key):** every metabase apply runs `Job/metabase-register` (`metabase/register-*.yaml`). It waits for Metabase, then:
* **fresh Metabase:** completes the setup wizard through the API with the admin from `Secret/metabase-admin`, then adds the DuckDB databases below with scans turned off;
* **already set up:** signs in as that admin and adds each one only when it's missing (existing ones are never modified);

| Metabase database | DuckFlight dbname | Catalog |
|---|---|---|
| `Lake` | `datalake` | RustFS built-in catalog (`datalake` table bucket): generator / maintenance tables |
| `Lakehouse` | `lakehouse` | Polaris (`lakehouse` bucket): tables of BeziSync's `analytics-lakehouse` destination |

(`METABASE_DATABASES` in `metabase/register-config.yaml`.)

DuckFlight silently falls back to the default catalog (`datalake`) for an unknown dbname, so a database whose catalog isn't attached would connect fine while showing the wrong tables. The Job therefore first checks, through `Lake`, that duckdb really has the catalog (`duckdb_databases()`), waiting up to `METABASE_CATALOG_WAIT_SECONDS` (480). It doesn't add a database whose catalog never appears, and it re-syncs the schema of existing ones once their catalog is there.
* **set up by hand with another admin:** sign-in fails → warning, skip. To adopt that instance, put its admin email/password into `Secret/metabase-admin` and re-apply.

`Secret/metabase-admin` is generated once by CI (email `admin@analytics.local`, random password) and never overwritten. Get the password to sign in:
```bash
kubectl get secret metabase-admin -n analytics -o jsonpath='{.data.password}' | base64 -d; echo
```
A failed DuckFlight connection test fails the Job; a Metabase that is not healthy within 10 min is a warning.

To add the lake by hand instead (Admin → Databases → Add → **PostgreSQL**):

| Field | Value |
|---|---|
| Host / Port | `duckdb.analytics.svc.cluster.local` / `5433` |
| Database name | **`datalake`** — DuckFlight uses `dbname` as the default catalog. With any other name the lake tables are not listed and `schema.table` SQL fails (`events.x` is rewritten to `<dbname>.events.x`) |
| Username / Password | the `DUCKFLIGHT_USER` / `DUCKFLIGHT_PASSWORD` GitHub Secrets |
| Use SSL / SSL mode | on / **`require`** — the cert is self-signed and regenerated on every duckdb pod start, so `verify-ca`/`verify-full` can never pass |

Then, in the same form's advanced options, turn off **"Rerun queries for simple explorations"** and set scanning for filter values to **"Never"** / only when adding a new filter widget: every scan and fingerprint is a full read of the Iceberg tables.

Validated locally on 2026-11-09 (DuckDB 2.0.0 + RustFS 1.0.4 + Metabase v0.65.3.6): sync lists `events.*` with typed columns, and both native SQL and GUI-built questions (e.g. count by `day(ts)` × column) run through DuckFlight. A table with no snapshot can show a placeholder `__` column on the very first sync — a later sync (Admin → Databases → Sync now) picks up the real schema.

## Prerequisites

* `kubectl` + `kustomize` (kubectl 1.14+ embeds it) and **helm 3** (helmCharts inflation requires the helm binary)
* VPA controllers for the duckdb VPA — official SIG-autoscaling chart: `helm repo add autoscaler https://kubernetes.github.io/autoscaler && helm install vpa autoscaler/vertical-pod-autoscaler --version 0.13.0`. Without the `verticalpodautoscalers` CRD, `kubectl apply -k duckdb/` fails on the VPA object
* `longhorn` StorageClass
* `tailscale` operator with Service annotations `tailscale.com/expose`
* Namespaces are created by `namespace.yaml` (root), `rustfs/namespace.yaml` and `polaris/namespace.yaml`; CI also ensures them idempotently

## Usage

> **Note:** `rustfs/` and `polaris/` use the helm chart inflator, so builds need `--enable-helm` and the helm binary. `kubectl apply -k` does not support `--enable-helm` — build first, then apply the rendered output (the CI workflow does exactly this):

```bash
kubectl kustomize --enable-helm . > built.yaml
kubectl apply -f built.yaml
kubectl diff -f built.yaml   # should be empty after apply
```

Note: `duckdb` references `rustfs-svc.rustfs.svc.cluster.local`, so applying the full stack leaves duckdb CrashLooping briefly until rustfs is ready and the sync script has run — it self-heals. Strict order if you want to avoid it:

```bash
kubectl kustomize --enable-helm rustfs/ | kubectl apply -f -
kubectl apply -k duckdb/
bash scripts/sync-duckdb-rustfs.sh
```

### Tailscale exposure

Metabase is always on the tailnet. To also put `duckdb` (DBeaver/psql from your laptop) or `rustfs` (console, S3 tools) there, render with `scripts/build-manifests.sh`, which layers the matching `tailscale/<name>` component on top:

```bash
scripts/build-manifests.sh . none   > built.yaml   # metabase only (default)
scripts/build-manifests.sh . duckdb > built.yaml   # + duckdb
scripts/build-manifests.sh . all    > built.yaml   # + duckdb + rustfs
```

In CI this is the `tailscale_expose` input. **Exposure is per apply:** an apply rendered without a component removes that Service's annotations, and the Tailscale operator tears the proxy down. So a later routine `apply` with the default `none` takes `duckdb` back off the tailnet. Re-pass the option on every apply where you want it kept.

Per-component (duckdb alone needs no helm):

```bash
kubectl apply -k duckdb/
```

## CI/CD Workflow (`.github/workflows/deploy.yaml`)

Manual dispatch (also `workflow_call`). Inputs:

* `module`: `all` (root kustomization), `duckdb`, `rustfs`, `polaris`, `maintenance`, `generator`, `metabase`
* `action`: `apply` | `diff` | `delete` | `dry-run` | `restart` | `logs` | `probe` | `sigprobe` | `trigger` | `repair`
* `force_rebuild` (boolean, default `false`): build both images even when change detection finds them already up to date
* `tailscale_expose` (`none` | `duckdb` | `rustfs` | `all`, default `none`): also put those Services on the tailnet for this apply/diff/dry-run (metabase always is). Only Services inside the chosen `module` are affected; see [Tailscale exposure](#tailscale-exposure)

Pipeline: **build-console** runs alongside **build-image** for `module` `all` / `polaris`: it builds the Polaris Console from the upstream commit pinned in `polaris/kustomization.yaml`, only when GHCR has no image with that tag yet (or `force_rebuild`). **build-image** (change-gated per image — a build only runs when GHCR's `:<sha>`-tagged history shows the image's inputs changed since its last push; `maintenance/image/**` gates the Spark image, `generator/**` minus `*.yaml` the generator. Re-running the same commit, or dispatching after a manifest/ConfigMap-only change, restores the cached layers instead of pushing identical images; see `force_rebuild`) → **execute**: validate module → checkout → kubectl v1.30 → helm v3.14 → Tailscale login (`tag:git`) → write kubeconfig from the `KUBECONFIG` secret → ensure namespaces → create secrets from GitHub Secrets → `scripts/build-manifests.sh` build (`kubectl kustomize --enable-helm` + `tailscale_expose` components) → execute action (rendered manifests are applied with `apply -f`, since `kubectl apply -k` cannot inflate helm charts).

Behavior notes:

* `apply` always runs the sync script afterwards (idempotent — no restart when already aligned), so rustfs changes propagate to duckdb automatically
* `restart` maps: `duckdb` → `deployment/duckdb -n analytics`; `rustfs` → `statefulset/rustfs -n rustfs`; `maintenance` → `iceberg-maintenance-daily`; `metabase` → `deployment/metabase -n analytics`; `all` → everything (targets are passed to kubectl as single `"kind/name -n ns"` units)
* `delete` deletes the module's rendered kustomization (`--ignore-not-found`)
* `logs` dumps recent pod logs for the module plus the two newest CronJob jobs in `analytics` — read-only cluster diagnosis from CI
* `probe` (generator) runs a read-only diagnostic inside the generator-stream pod: credential shape (never values), resolved package inventory, a real `load_iceberg_catalog()` attempt with full traceback, and a signed-request matrix against the live catalog endpoint

Required GitHub Secrets:

| Secret | Purpose |
|---|---|
| `KUBECONFIG` | Kubeconfig reaching the cluster (Tailscale) |
| `TAILSCALE_CLIENT_ID`, `TAILSCALE_AUTH_KEY` | Tailscale OAuth for the runner |
| `RUSTFS_ACCESS_KEY`, `RUSTFS_SECRET_KEY` | Stamped into `rustfs-credentials` in **both** namespaces |
| `DUCKFLIGHT_USER`, `DUCKFLIGHT_PASSWORD` | Stamped into `duckflight-auth` |

## Secrets

* No Secret manifests are stored in the repo — CI creates/updates them from GitHub Secrets on every run (`kubectl create secret ... --dry-run=client -o yaml | kubectl apply -f -`)
* Exception: `metabase-secrets` is generated in-cluster (`openssl rand`) only when absent and never overwritten — no GitHub Secret, and deliberately not rotatable by re-running CI
* Also generated once: `metabase-admin` (ns `analytics`, the Metabase admin the register Job sets up and signs in as) and `rustfs-bezisync` (ns `rustfs`, the RustFS user BeziSync writes with).
* Rotating `rustfs-bezisync`: delete the Secret, re-apply `module=rustfs` (the Job upserts the user with the new key), then re-run bezisync-lab's playground apply, which updates its `analytics-lake` and `analytics-lakehouse` destinations with the new key
* Rotating a Polaris principal (`polaris-duckdb` / `polaris-bezisync` / `polaris-admin`): delete the Secret and re-apply `module=polaris`. `Job/polaris-setup` resets the principal to the new pair. Then restart duckdb (`action=restart module=duckdb`), re-run bezisync-lab's playground apply (for `polaris-bezisync`), or log in to the console with the new `admin` secret
* Rotate: update the GitHub Secret, run this workflow with `action=apply`, then `action=restart` with `module=duckdb` (rustfs picks up creds on its own restart)

## Maintenance Notes

* 1 file per resource — `git log -- <file>` isolates history
* duckdb memory sizing is VPA-driven: the request → Downward API env → `init.sql` × 85% is the only place `memory_limit` is set; add no container memory limit and don't hardcode a `SET memory_limit` value
* Helm chart upgrades: bump `version:` in `rustfs/kustomization.yaml` / `polaris/kustomization.yaml`; helm values live in each module's `helm.yaml` only. For Polaris, keep the admin-tool tag in `polaris/helm.yaml` (`bootstrap-realm`) equal to the chart's app version
* Changing `drivesPerNode` on an existing RustFS StatefulSet is not allowed (chart rule) — topologies are fixed after first deploy
* The `duckdb-settings` ConfigMap is kustomize-managed with static defaults; the sync script's patch is the only external mutation and is re-derived on every `apply`
