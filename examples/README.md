# Examples

Write ordinary Flyte 2 Python. Each `@env.task` runs in an Armada-scheduled pod. The only
Armada-specific line is `plugin_config=ArmadaConfig(queue=...)`. Everything else (resources,
chaining, fan-out, typed data) is stock Flyte.

Five examples that build up in order. Start at the top and work down: each adds one idea, ending with
a gang inside a DAG.

| File | Shows | Expected output |
| --- | --- | --- |
| [`hello.py`](hello.py) | **Hello world.** One task, returns a greeting string. | `hello armada, from an Armada pod` |
| [`function.py`](function.py) | **Simple.** One task that does real work: a Black-Scholes option price. | `call price = 10.4506` |
| [`fanout.py`](fanout.py) | **Parallel.** A typed dataclass through a fan-out / fan-in (independent jobs via `asyncio.gather`). | `Stats(...)  mean = 506.47` |
| [`gang.py`](gang.py) | **Armada gangs.** N co-dependent workers, scheduled all-or-nothing (the one feature plain k8s cannot give you). | `global average = 54.32` |
| [`dag.py`](dag.py) | **Gang in a DAG.** Generate a dataset, run a co-scheduled gang over it, aggregate the results. | `global mean = 50.00` |

## Run one

The runner submits the example through the Flyte backend. Each example prints its typed result and a
link to the run in the Flyte UI:

```bash
./.venv/bin/python examples/hello.py
```

Run any example the same way. Prerequisite: a Flyte 2 backend with the connector. The
[quickstart](../demo/) walks the setup end to end, or point at your own
([../docs/getting-started.md](../docs/getting-started.md)).

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
`_runner.py` is the shared helper the examples call to run. It is not an example itself.
