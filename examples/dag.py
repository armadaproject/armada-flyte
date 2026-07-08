"""Armada-specific: a DAG with a gang in the middle.

    generate(n)          one job builds a shared dataset
    calc (x N) ONE GANG  N co-scheduled workers, each on a slice, all-or-nothing
    aggregate(parts)     one job folds the results

The generate -> gang -> aggregate shape: an upstream job feeds a co-dependent cohort (a gang, sharing
a gang_id and gang_cardinality = N, so Armada runs them together or leaves them queued), and a
downstream job combines their results. generate and aggregate are ordinary jobs, outside the gang.
Contrast fanout.py, whose middle stage is independent and uses no gang.

    ./.venv/bin/python examples/dag.py
"""

from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass

import flyte
from armada_flyte import ArmadaConfig

IMAGE = os.environ.get("ARMADA_TASK_IMAGE", "armada-flyte-task:v1")

# Size of the gang. It must equal the number of fanned-out calc tasks, so both read this constant.
WORKERS = 4

# generate and aggregate are ordinary Armada jobs, no gang.
io = flyte.TaskEnvironment(
    name="io",
    image=IMAGE,
    resources=flyte.Resources(cpu=1, memory="512Mi"),
    plugin_config=ArmadaConfig(queue="flyte"),
)
# The calc workers all join one gang: same gang_id, same gang_cardinality = N, so Armada schedules
# them all-or-nothing together.
gang = flyte.TaskEnvironment(
    name="calc",
    image=IMAGE,
    resources=flyte.Resources(cpu=1, memory="512Mi"),
    plugin_config=ArmadaConfig(queue="flyte", gang_id="dag", gang_cardinality=WORKERS),
)
# The driver orchestrates the DAG. It runs as a backend pod, so it needs the same task image.
driver = flyte.TaskEnvironment(name="driver", image=IMAGE, depends_on=[io, gang])


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


@gang.task
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
    # Fan out EXACTLY WORKERS gang members so the count matches gang_cardinality. Armada co-schedules
    # the whole gang all-or-nothing.
    parts = await asyncio.gather(*(calc(rank=r, chunk=chunks[r]) for r in range(WORKERS)))
    return await aggregate(parts=list(parts))


if __name__ == "__main__":
    from _runner import run

    result: float = run(pipeline, n=8000)
    print(f"\nglobal mean = {result:.2f}  (generate -> {WORKERS}-worker Armada gang -> aggregate)")
