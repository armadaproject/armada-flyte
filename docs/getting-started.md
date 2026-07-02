# Getting started

From zero to a Flyte 2 task running on Armada and showing up in the **Flyte UI**, on a local stack.

The end state: you write a `@env.task`, submit it through a Flyte backend, and watch Armada
schedule and run it as a pod in the Flyte console.

## 1. Bring up Armada (a local Kind cluster)

The connector submits to a running Armada cluster. From a checkout of the
[armada](https://github.com/armadaproject/armada) repo, create a local Kubernetes cluster and start
the stack with a real executor:

```
mage Kind            # create the Kind cluster "armada-test" and select its kube context
mage dev:up no-auth  # dependencies, migrations, and the full Armada stack with a real executor
```

`mage Kind` selects the `kind-armada-test` context, which the executor uses to create pods. Wait
until the executor logs `Reporting current free resource` and the scheduler logs
`Retrieved 1 executors` before submitting (jobs sent in the first ~60s fail with
`Retrieved 0 executors`, because the scheduler refreshes its executor list every 60s).

Create the queue the examples target, and give it a couple of seconds to propagate:

```
go run cmd/armadactl/main.go create queue flyte
```

Tear the cluster down later with `mage KindTeardown`, and stop the dependencies with `mage dev:down`.

## 2. Bring up a Flyte backend (a second cluster)

This is a separate cluster from Armada's, and that is by design. Armada runs your job pods on its own
kind cluster (step 1). The Flyte backend runs in its own k3d cluster, a "devbox" that holds the Flyte
control plane, the UI, and a blob store. It routes `armada` tasks to the connector, which submits them
to Armada, so the pods land on the Armada cluster. The two clusters talk through the connector service.

Stock Flyte 2 neither registers the Armada connector plugin nor routes `armada` tasks to it. The
`armada-devbox` branch adds both: the connector plugin (which is
[dejanzele/flyte#1](https://github.com/dejanzele/flyte/pull/1), upstreaming in progress) plus the
devbox config that routes `armada` tasks to the connector. Build the devbox from it. The build needs
Docker (with buildx), Helm, Kustomize, and Go on your PATH:

```
git clone -b armada-devbox https://github.com/dejanzele/flyte.git
cd flyte
make devbox-build    # one-time: builds the devbox image with the connector plugin and routing (a heavy build)
make devbox-run      # starts the devbox; the Flyte UI comes up on http://localhost:30080
```

On this branch the devbox does not publish its Kubernetes API port, so it coexists with the Armada
cluster (which uses host port 6443). Inspect the devbox cluster with `docker exec flyte-devbox
kubectl --kubeconfig /etc/rancher/k3s/k3s.yaml ...`. Once `http://localhost:30080` answers, continue.
Stop the devbox later with `docker rm -f flyte-devbox`. [../demo/](../demo/) describes what the run
script expects from it.

## 3. Install this package

On Apple Silicon use an arm64 Python. An x86_64 interpreter cannot load Flyte's native `obstore`
wheel. From a checkout of this repo:

```
python3.11 -m venv .venv
./.venv/bin/pip install -e ".[dev]"
```

To use the connector in your own project instead, install it directly:

```
pip install "armada-flyte @ git+https://github.com/dejanzele/armada-flyte.git"
```

## 4. Submit your first task

With Armada up (step 1) and a Flyte backend up (step 2), one command builds the task image, wires the
blob store, starts the connector service, and submits the example through the backend:

```
./demo/run.sh examples/hello.py
```

It prints a UI link and the result:

```
submitted run rf6zwrmnpzpwdgnfzffn
  UI: http://localhost:30080/v2/.../runs/rf6zwrmnpzpwdgnfzffn
hello armada, from an Armada pod
```

Open the `UI:` link to watch the run: Armada schedules the pod, runs the `@env.task`, and records the
typed result. Then try the other examples through the backend, for example
`./demo/run.sh examples/fanout.py` (a parallel fan-out) and `./demo/run.sh examples/gang.py` (an
all-or-nothing gang). The example surface is described in [../examples/](../examples/).

## Configuration

The connector's settings (endpoint, the blob store the pods use, and later auth/TLS) resolve in
this order, lowest to highest: built-in defaults, then the environment, then in-code overrides.

- **Environment**: `ARMADA_URL` (default `localhost:50051`, the Armada submit/status gRPC endpoint),
  and `FLYTE_BLOB_ENDPOINT` / `FLYTE_BLOB_ACCESS_KEY` / `FLYTE_BLOB_SECRET_KEY` for the blob store.
  The run scripts set these for you.
- **In code**: `armada_flyte.configure(armada_url=...)`, called before the first task runs. This is
  the home for credentials (it never reaches your task config or the control-plane DB). For the
  backend, call it in the connector service launcher.

Settings resolve lazily on first use, so `configure()` works after `import armada_flyte` (no
set-the-env-var-before-import ordering trap).

Other knobs:

- `ARMADA_TASK_IMAGE` (default `armada-flyte-task:v1`): the task image the runner builds and loads.

If something does not behave, check [gotchas.md](gotchas.md) first. Most setup problems are
listed there.
