"""DAG with a gang in the middle.

    generate(n)          one job builds a shared dataset
    calc (x N) ONE GANG  N co-scheduled workers, each on a slice, all-or-nothing
    aggregate(parts)     one job folds the results

The generate -> gang -> aggregate shape: an upstream job feeds a co-dependent cohort (a gang, formed
with armada_flyte.Gang, so Armada runs all N together or leaves them queued), and a downstream job
combines their results. generate and aggregate are ordinary jobs, outside the gang.
Contrast fanout.py, whose middle stage is independent and uses no gang.

    ./.venv/bin/python examples/dag.py
"""

from __future__ import annotations

import os
from dataclasses import dataclass

import flyte
from armada_flyte import ArmadaConfig, Gang

IMAGE = os.environ.get("ARMADA_TASK_IMAGE", "armada-flyte-task:v1")

# Size of the gang. Written once: Gang derives the cardinality from the members added at the fan-out.
WORKERS = 4

# The node label the gang members must share a value of. With kubernetes.io/hostname the whole gang
# lands on one node, so the sum of the members' requests must fit that node's free capacity.
UNIFORMITY_LABEL = "kubernetes.io/hostname"

# generate and aggregate are ordinary Armada jobs, no gang.
io = flyte.TaskEnvironment(
    name="io",
    image=IMAGE,
    resources=flyte.Resources(cpu=1, memory="512Mi"),
    plugin_config=ArmadaConfig(queue="flyte"),
)
# An ordinary Armada env for the calc workers. The gang is expressed at the fan-out in pipeline().
calc_env = flyte.TaskEnvironment(
    name="calc",
    image=IMAGE,
    # Small resources: with hostname uniformity the whole gang lands on one node.
    resources=flyte.Resources(cpu="500m", memory="256Mi"),
    plugin_config=ArmadaConfig(queue="flyte"),
)
# The driver orchestrates the DAG. It runs as a backend pod, so it needs the same task image.
driver = flyte.TaskEnvironment(name="driver", image=IMAGE, depends_on=[io, calc_env])


@dataclass
class Partial:
    rank: int
    total: float
    count: int


@io.task
async def generate(n: int, seed: int) -> list[float]:
    """Upstream job: produce the shared dataset the gang works on."""
    import random

    rng = random.Random(seed)
    return [rng.uniform(0, 100) for _ in range(n)]


@calc_env.task
async def calc(rank: int, chunk: list[float]) -> Partial:
    """One gang member: its slice's partial sum. A real distributed job would exchange results with
    its peers each round, which is why the workers must be co-scheduled."""
    return Partial(rank=rank, total=sum(chunk), count=len(chunk))


@io.task
async def aggregate(parts: list[Partial]) -> float:
    """Downstream job: fold the gang's partials into the global mean."""
    return sum(p.total for p in parts) / sum(p.count for p in parts)


@driver.task
async def pipeline(n: int = 8000) -> float:
    data = await generate(n=n, seed=42)
    chunks = [data[i::WORKERS] for i in range(WORKERS)]
    # The calc stage is one gang: add each worker, then run() submits them all-or-nothing onto nodes
    # sharing UNIFORMITY_LABEL. Gang derives the id and cardinality from the members.
    gang = Gang(node_uniformity_label=UNIFORMITY_LABEL)
    for rank in range(WORKERS):
        gang.add(calc, rank=rank, chunk=chunks[rank])
    parts = await gang.run()
    return await aggregate(parts=list(parts))


if __name__ == "__main__":
    from _runner import run

    result: float = run(pipeline, n=8000)
    print(f"\nglobal mean = {result:.2f}  (generate -> {WORKERS}-worker Armada gang -> aggregate)")
