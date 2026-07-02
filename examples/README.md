# Examples

Write ordinary Flyte 2 Python. Each `@env.task` runs in an Armada-scheduled pod. The only
Armada-specific line is `plugin_config=ArmadaConfig(queue=...)`; everything else (resources,
chaining, fan-out, typed data) is stock Flyte.

Five examples, in order:

| File | Shows | Expected output |
| --- | --- | --- |
| [`hello.py`](hello.py) | **Hello world.** One task, returns a greeting string. | `hello armada, from an Armada pod` |
| [`function.py`](function.py) | **Simple.** One task that does real work: a Black-Scholes option price. | `call price = 10.4506` |
| [`fanout.py`](fanout.py) | **Parallel.** A typed dataclass through a fan-out / fan-in (independent jobs via `asyncio.gather`). | `Stats(...)  mean = 506.47` |
| [`ml_pipeline.py`](ml_pipeline.py) | **Complex.** End-to-end ML: make data, k-fold cross-validation in parallel, pick the best model, evaluate. | `best alpha = 1.0  fit y ~ 3.00 x + 2.00` |
| [`gang.py`](gang.py) | **Armada gangs.** N co-dependent workers, scheduled all-or-nothing (the one feature plain k8s cannot give you). | `global average = 54.32` |

## Run one

The runner builds the task image, wires the blob store, and submits the example through the Flyte
backend, so the run shows up in the Flyte UI:

```bash
./demo/run.sh examples/hello.py
```

Pass any example as the argument. Prerequisite: a running Armada cluster and a Flyte 2 backend (see
[../docs/getting-started.md](../docs/getting-started.md)).

## What you write

```python
import flyte
from armada_flyte import ArmadaConfig

env = flyte.TaskEnvironment(
    name="hello",
    image="armada-flyte-task:v1",
    resources=flyte.Resources(cpu=1, memory="512Mi"),   # required; declared the stock-Flyte way
    plugin_config=ArmadaConfig(queue="flyte"),           # the one Armada-specific line
)

@env.task
async def greet(name: str) -> str:
    return f"hello {name}, from an Armada pod"
```

Resources are required (Armada rejects a job without them). Declare them with `flyte.Resources`
on the environment, or per task with `@env.task(resources=...)`. Need a GPU? `flyte.Resources(gpu=1)`.
`_runner.py` is the shared helper the examples call to run; it is not an example itself.
