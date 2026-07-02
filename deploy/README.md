# Running the connector as a service

The examples run on a Flyte backend: the connector runs there as a long-running gRPC service that
FlytePropeller routes `armada` tasks to. This page is about running that service.

## Run the service locally

```bash
c0 --modules armada_flyte.connector        # serves the connector on :8000
```

`armada-flyte` declares a `flyte.connectors` entry point, so once it is installed the connector
also loads with a bare `c0` (no `--modules`). On startup the service prints its registered task
types:

```
Connector Name     Support Task Types
Armada Connector   armada (0)
```

Point it at Armada with `ARMADA_URL` (default `localhost:50051`).

## Deploy it to a Flyte backend

`deploy/app.py` defines the connector as a `flyte.app.ConnectorEnvironment`. Against a Flyte
backend (`flyte.init_from_config()` pointed at one), deploy it with:

```bash
python deploy/app.py        # calls flyte.deploy(connector)
```

This builds the image and creates the connector deployment. After that, a task whose
`task_type` is `armada` is routed to it automatically, so workflows run without local execution.
A deployed Flyte backend is the one prerequisite this repo does not stand up for you.

## Python function tasks through the backend

Running an example with `BACKEND=1` (which `./demo/run.sh` sets for you) registers the `@env.task`
with the backend and runs it on Armada, so it appears in the Flyte UI with its true output. Two
things matter here that local execution hides:

- **One shared blob store.** The backend, the client, and the Armada pods must all read and write
  the same bucket. Point the connector service at it (`FLYTE_BLOB_ENDPOINT` /
  `FLYTE_BLOB_ACCESS_KEY` / `FLYTE_BLOB_SECRET_KEY`) at an address the Armada pods can reach. If the
  Armada cluster is separate from the backend, use the store's externally reachable endpoint.
- **Runtime arguments.** For a backend run, FlytePropeller normally fills the `a0` runtime args
  (`--run-base-dir`, `--org`/`--project`/`--domain`, and the run/action names). The connector fills
  them itself from the task execution metadata, so the function runs and its typed output is
  recorded.
