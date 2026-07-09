# Running the connector as a service

The examples run on a Flyte backend, where the connector runs as a long-running gRPC service. The
executor's connector-service plugin routes `armada` tasks to it. This page covers running the
connector and wiring a backend to route to it.

## Run the service locally

```bash
c0 --modules armada_flyte.connector        # serves the connector on :8000
```

`armada-flyte` declares a `flyte.connectors` entry point, so once it is installed the connector also
loads with a bare `c0` (no `--modules`). On startup the service prints its registered task types:

```
Connector Name     Support Task Types
Armada Connector   armada (0)
```

Point it at Armada with `ARMADA_URL` (default `localhost:50051`), and at the blob store the Armada
pods use with `FLYTE_BLOB_ENDPOINT` / `FLYTE_BLOB_ACCESS_KEY` / `FLYTE_BLOB_SECRET_KEY`.

## What your Flyte backend needs

Stock Flyte 2 does not route `armada` tasks to the connector out of the box. Your backend needs both
the connector-service plugin registered and the routing config.

- **The plugin registered.** Stock `flyte-binary-v2` registers the connector-service plugin as of
  [flyteorg/flyte#7565](https://github.com/flyteorg/flyte/pull/7565) (any build from that commit on).
  On an earlier build the plugin is absent and `armada` tasks have no handler.
- **Routing to the connector.** Declare the connector and route the `armada` task type to it. For the
  `flyte-binary` chart this goes in the config:

  ```yaml
  configuration:
    inline:
      plugins:
        connector-service:
          defaultConnector:
            endpoint: "dns:///<connector-host>:8000"   # where c0 listens
            insecure: true
          supportedTaskTypes:
            - armada
  enabled_plugins:
    tasks:
      task-plugins:
        enabled-plugins: [container, sidecar, connector-service, echo]
        default-for-task-types:
          armada: connector-service
  ```

  `supportedTaskTypes: [armada]` binds the `armada` task type to the connector. Without it the backend
  fails the task with `no connector found for task type [armada]`. In the `flyte-binary` chart this
  must live under `configuration.inline`; the chart's top-level `configuration.connectorService` is
  not wired into the config.

## Deploy the connector into the backend

Running `c0` on a host is one option. To run the connector inside the backend cluster instead,
`deploy/app.py` defines it as a `flyte.app.ConnectorEnvironment`. Against a Flyte backend
(`flyte.init_from_config()` pointed at one), deploy it with:

```bash
python deploy/app.py        # calls flyte.deploy(connector)
```

This builds the image and creates the connector deployment. After that, a task whose `task_type` is
`armada` is routed to it automatically.

## Two things a backend run depends on

- **One shared blob store.** The backend, the client, and the Armada pods must all read and write the
  same bucket. Point the connector at an endpoint the Armada pods can reach.
- **Runtime arguments.** Flyte 1 filled the `a0` runtime args (`--run-base-dir`, `--org` / `--project`
  / `--domain`, and the run and action names) inside FlytePropeller. Flyte 2 has no FlytePropeller, so
  the connector fills them itself from the task execution metadata. The function runs and its typed
  output is recorded.
