"""Various: a typed multi-stage pipeline with a parallel fan-out / fan-in.

    generate(n)        produce n numbers
    split into S shards
    shard_stats(chunk) per-shard Stats in S PARALLEL Armada pods (fan-out via asyncio.gather)
    merge(parts)       combined Stats (fan-in)

The shards are INDEPENDENT jobs: Armada schedules each as capacity frees up, which is the right
primitive for embarrassingly-parallel work (a parameter sweep, Monte-Carlo paths, batch scoring).
A typed dataclass (Stats) flows between the stages. Run:

    ./demo/run.sh examples/fanout.py                 # default: runs on Armada, shows in the Flyte UI
"""

from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass

import flyte
from armada_flyte import ArmadaConfig

IMAGE = os.environ.get("ARMADA_TASK_IMAGE", "armada-flyte-task:v1")

work = flyte.TaskEnvironment(
    name="stats",
    image=IMAGE,
    resources=flyte.Resources(cpu=1, memory="512Mi"),
    plugin_config=ArmadaConfig(queue="flyte"),
)
# The driver orchestrates the fan-out / fan-in. It runs as a backend pod, so it needs the same
# task image.
driver = flyte.TaskEnvironment(name="driver", image=IMAGE, depends_on=[work])


@dataclass
class Stats:
    count: int
    total: int
    lo: int
    hi: int


@work.task
async def generate(n: int, seed: int) -> list[int]:
    import random

    rng = random.Random(seed)
    return [rng.randint(1, 1000) for _ in range(n)]


@work.task
async def shard_stats(chunk: list[int]) -> Stats:
    return Stats(count=len(chunk), total=sum(chunk), lo=min(chunk), hi=max(chunk))


@work.task
async def merge(parts: list[Stats]) -> Stats:
    return Stats(
        count=sum(p.count for p in parts),
        total=sum(p.total for p in parts),
        lo=min(p.lo for p in parts),
        hi=max(p.hi for p in parts),
    )


@driver.task
async def pipeline(n: int = 2000, shards: int = 4) -> Stats:
    data = await generate(n=n, seed=42)
    chunks = [data[i::shards] for i in range(shards)]
    parts = await asyncio.gather(*(shard_stats(chunk=c) for c in chunks))
    return await merge(parts=list(parts))


if __name__ == "__main__":
    from _runner import run

    result: Stats = run(pipeline, n=2000, shards=4)
    print(f"\n{result}\nmean = {result.total / result.count:.2f}  (computed across parallel Armada pods)")
