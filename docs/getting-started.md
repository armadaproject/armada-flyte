# Getting started

`armada-flyte` is a Flyte 2 connector. It runs your Flyte tasks as Armada jobs.

## Try it locally

The fastest way to see it work is the demo. [../demo/](../demo/) stands up a Flyte 2 backend and the
connector in the Kind cluster Armada already uses. With Armada running, `./demo/setup.sh` builds the
backend and starts the connector, then `./.venv/bin/python examples/hello.py` runs a task on Armada
and shows it in the Flyte UI. Follow [../demo/README.md](../demo/README.md) for the walkthrough.

The rest of this page is for running the connector against a Flyte 2 backend you already operate. That
needs two things running: an **Armada** cluster (the connector submits jobs to it) and a **Flyte 2
backend** whose executor routes `armada` tasks to the connector (see
[../deploy/README.md](../deploy/README.md) for what the backend needs).

## Install the connector

On Apple Silicon use an arm64 Python. An x86_64 interpreter cannot load Flyte's native `obstore`
wheel. From a checkout of this repo:

```
python3.11 -m venv .venv
./.venv/bin/pip install -e ".[dev]"
```

Or install it into your own project:

```
pip install "armada-flyte @ git+https://github.com/dejanzele/armada-flyte.git"
```

## Run the connector

The connector is a gRPC service. Run it where your Flyte backend can reach it:

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

To run the connector inside the backend cluster instead of on a host, deploy it with `deploy/app.py`
(see [../deploy/README.md](../deploy/README.md)).

## Submit a task

Tasks run in a generic image (`armada-flyte-task:v1` by default) that `a0` fast-registers your code
into at runtime, so one image serves every example. Build it once from the repo root and make it
available to the Armada cluster (`kind load docker-image armada-flyte-task:v1 --name <cluster>`, or
push to a registry the cluster can pull from):

```
docker build -t armada-flyte-task:v1 -f- . <<'EOF'
FROM python:3.11-slim
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir "flyte==2.5.1" .
EOF
```

The examples target a backend at `localhost:30080` (adjust `flyte.init(...)` in
[examples/_runner.py](../examples/_runner.py) for yours) and queue `flyte`:

```
./.venv/bin/python examples/hello.py
```

It prints a UI link. Open it to watch Armada schedule the pod, run the `@env.task`, and record the
typed result. Then try `examples/fanout.py` (a parallel fan-out) and `examples/gang.py` (an
all-or-nothing gang). The example surface is described in [../examples/](../examples/).

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
