# Demo: a Python function on Armada, through the Flyte backend

`./demo/run.sh` runs an ordinary Flyte 2 task on Armada via a Flyte backend. The task is registered
with FlyteAdmin, scheduled by Armada as a pod, and shows up in the Flyte UI with its result:

```
submitted run rf6zwrmnpzpwdgnfzffn
  UI: http://localhost:30080/v2/.../runs/rf6zwrmnpzpwdgnfzffn
hello armada, from an Armada pod
```

## What you write

A stock `@env.task` (see the [README](../README.md#the-whole-integration)); the only Armada-specific
line is `plugin_config=ArmadaConfig(...)` on the environment. The demo registers it with the backend
via `flyte.run`, so the run appears in the Flyte UI.

## Run it

```bash
./demo/run.sh
```

The script builds the task image, loads it into the Armada cluster, points the connector at the
backend's blob store, and runs the task. It prints the UI link and the result.

Run a different example by passing it, for example the multi-stage ML pipeline (make data,
parallel cross-validation, fit, evaluate), which runs end to end through the backend:

```bash
./demo/run.sh examples/ml_pipeline.py
```

## One-time prerequisites

The script does the wiring; it assumes two things are already running:

1. **An Armada cluster** with a real executor on a kind cluster. From a checkout of the
   [armada](https://github.com/armadaproject/armada) repo:

   ```bash
   mage Kind            # kind cluster (default name: armada-test)
   mage dev:up no-auth  # Armada with a real executor
   ```

2. **A Flyte 2 backend** in its own cluster, separate from Armada's, whose executor registers the
   Armada connector plugin and routes `armada` tasks to it. Build and run it from the `armada-devbox`
   branch of [dejanzele/flyte](https://github.com/dejanzele/flyte) with `make devbox-build` then
   `make devbox-run`. [../docs/getting-started.md](../docs/getting-started.md) step 2 has the details.

Override defaults via env vars: `KIND_CLUSTER`, `DEVBOX`, `ARMADA_URL`, `HOST_IP`.

## What the script hides

The two things backend execution needs beyond local execution, both handled for you:

- **One shared blob store.** The backend, the client, and the Armada pods all use the backend's
  bucket. The pods reach it through its host-published port; the script reads the endpoint and
  credentials and passes them to the connector.
- **Runtime arguments.** For a backend run FlytePropeller normally fills the task's runtime args
  (`--run-base-dir`, org/project/domain, run and action names). The connector fills them from the
  task execution metadata, so the function runs and its typed output is recorded.

See [../docs/architecture.md](../docs/architecture.md#python-function-tasks) and
[../deploy/README.md](../deploy/README.md) for the full picture.
