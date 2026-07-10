"""Hello world: the smallest Armada task. One @env.task, one line of Armada config.

    ./.venv/bin/python examples/hello.py                  # runs on Armada, shows in the Flyte UI

Next: function.py does real work, then fanout.py, gang.py, and dag.py build up to a gang inside a DAG.
See examples/README.md for the ordered tour.
"""

from __future__ import annotations

import os

import flyte
from armada_flyte import ArmadaConfig

IMAGE = os.environ.get("ARMADA_TASK_IMAGE", "armada-flyte-task:v1")

# The connector submits to the Armada at $ARMADA_URL (default localhost:50051). Point it elsewhere
# with that env var, or in code: armada_flyte.configure(armada_url="armada.example.com:50051").

env = flyte.TaskEnvironment(
    name="hello",
    image=IMAGE,
    resources=flyte.Resources(cpu=1, memory="512Mi"),
    plugin_config=ArmadaConfig(queue="flyte"),   # the one Armada-specific line
)


@env.task
async def greet(name: str) -> str:
    return f"hello {name}, from an Armada pod"


if __name__ == "__main__":
    from _runner import run

    print("\n" + run(greet, name="armada"))
