# Getting started

`armada-flyte` is a Flyte 2 connector. It runs your Flyte tasks as Armada jobs.

## Try it locally

The fastest way to see it work is the demo. [../demo/](../demo/) stands up a Flyte 2 backend and the
connector in the Kind cluster Armada already uses, then runs a task on Armada. Follow
[../demo/README.md](../demo/README.md) for the walkthrough.

The rest of this page is for running the connector against a Flyte 2 backend you already operate. That
needs two things running: an **Armada** cluster (the connector submits jobs to it) and a **Flyte 2
backend** whose executor routes `armada` tasks to the connector (see
[../deploy/README.md](../deploy/README.md) for what the backend needs).

## Install the connector

From a checkout of this repo (Python 3.10 or newer):

```
python3 -m venv .venv
./.venv/bin/pip install -e ".[dev]"
```

Or install it into your own project:

```
pip install "armada-flyte @ git+https://github.com/armadaproject/armada-flyte.git"
```

## Run the connector

The connector is a gRPC service. `c0` is Flyte's connector-runtime binary, and installing this repo
registers the Armada connector into it. Run it where your Flyte backend can reach it:

```
ARMADA_URL=<armada-host>:50051 \
  FLYTE_BLOB_ENDPOINT=<blob-endpoint> \
  FLYTE_BLOB_ACCESS_KEY=<key> FLYTE_BLOB_SECRET_KEY=<secret> \
  ./.venv/bin/c0 --port 8000
```

`ARMADA_URL` is where it submits jobs. `FLYTE_BLOB_*` is the blob store the Armada pods read and
write, at an address those pods can reach. On startup it prints the task types it serves:

```
Connector Name     Support Task Types
Armada Connector   armada (0)
```

To run the connector inside the backend cluster instead of on a host, deploy it as a Deployment with
[../deploy/kubernetes/connector.yaml](../deploy/kubernetes/connector.yaml) (see
[../deploy/README.md](../deploy/README.md)).

## Submit a task

Tasks run in a generic image (`armada-flyte-task:v1` by default). Flyte's task-runtime bootstrap `a0`
fast-registers your code into that image at runtime, so one image serves every example. Build it once
from the repo root and make it
available to the Armada cluster (`kind load docker-image armada-flyte-task:v1 --name <cluster>`, or
push to a registry the cluster can pull from):

```
docker build -t armada-flyte-task:v1 -f- . <<'EOF'
FROM python:3.13-slim
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir "flyte==2.5.8" .
EOF
```

The examples submit to `$FLYTE_ENDPOINT` (default `localhost:30080`) on queue `flyte`, and build the
run's UI link from `$FLYTE_UI_BASE`. Set both for your backend:

```
FLYTE_ENDPOINT=<flyte-host>:<port> FLYTE_UI_BASE=https://<your-console>/v2 \
  ./.venv/bin/python examples/hello.py
```

Each example prints its typed result and a link to the run in the Flyte UI (status, graph, logs). Work
through the rest in order: `examples/fanout.py` (a parallel fan-out), `examples/gang.py` (an
all-or-nothing gang), and `examples/dag.py` (a gang inside a DAG). See
[../examples/](../examples/) for the full set.

## Configuration

The connector's settings (endpoint, the blob store the pods use, and later auth/TLS) resolve in
this order, lowest to highest: built-in defaults, then the environment, then in-code overrides.

- **Environment**: `ARMADA_URL` (default `localhost:50051`, the Armada submit/status gRPC endpoint),
  and `FLYTE_BLOB_ENDPOINT` / `FLYTE_BLOB_ACCESS_KEY` / `FLYTE_BLOB_SECRET_KEY` for the blob store.
- **In code**: `armada_flyte.configure(armada_url=...)`, called before the first task runs. This is
  the home for credentials (it never reaches your task config or the control-plane DB). For the
  backend, call it in the connector service launcher.

Settings resolve lazily on first use, so `configure()` works after `import armada_flyte` (no
set-the-env-var-before-import ordering trap).

Other knobs:

- `ARMADA_TASK_IMAGE` (default `armada-flyte-task:v1`): the task image an example runs in.
