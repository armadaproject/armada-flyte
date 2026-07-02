"""Armada-specific: GANG scheduling (all-or-nothing co-scheduling of a distributed job).

    seed shards            give each worker a slice of a shared dataset
    worker (x N) ONE GANG  N co-dependent workers, scheduled all-or-nothing by Armada
    reduce                 combine the workers' contributions into the final result

The N workers form a GANG: they share a gang_id and all declare the same gang_cardinality = N, so
Armada places ALL N pods together or NONE. This is the right primitive for a distributed job whose
workers must run AT THE SAME TIME (an all-reduce ring, an MPI world, parameter-server + workers): a
worker that exchanges partial results every round cannot make progress while its peers are absent.
Gang scheduling means the job never holds half its workers idle: the whole cohort runs, or the gang
stays queued until it fits.

Contrast examples/fanout.py: those shards are INDEPENDENT and correctly use no gang (they should
start piecemeal as capacity frees up). Gangs are for co-dependent workers, not parallel busywork.

Gang placement is a property of the real Armada scheduler, so run this THROUGH THE BACKEND:

    ./demo/run.sh examples/gang.py
"""

from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass

import flyte
from armada_flyte import ArmadaConfig

IMAGE = os.environ.get("ARMADA_TASK_IMAGE", "armada-flyte-task:v1")

# Number of workers in the gang. The cardinality MUST equal the count of fanned-out worker tasks, so
# both read this one constant. Raise it (or the per-worker resources) past what the cluster can fit
# at once to watch the whole gang stay QUEUED as a unit instead of half-starting.
WORKERS = 4

# Every task in this env joins one gang: the connector scopes gang_id to the run, and all N members
# declare the same cardinality, so Armada schedules them all-or-nothing together. (Add
# gang_node_uniformity_label="kubernetes.io/hostname" to also force them onto co-located nodes.)
work = flyte.TaskEnvironment(
    name="gang",
    image=IMAGE,
    resources=flyte.Resources(cpu=1, memory="512Mi"),
    plugin_config=ArmadaConfig(queue="flyte", gang_id="distributed-average", gang_cardinality=WORKERS),
)
# The driver orchestrates the gang. It runs as a backend pod, so it needs the same task image.
# (Note the driver is not in the worker env, so it carries no gang_cardinality and is not itself a
# gang member.)
driver = flyte.TaskEnvironment(name="driver", image=IMAGE, depends_on=[work])


@dataclass
class Contribution:
    rank: int
    local_sum: float
    local_count: int


@work.task
async def worker(rank: int, n: int) -> Contribution:
    """One member of the gang: owns shard `rank` and computes its local contribution. A real
    distributed job would exchange partial results with its peers every round, which is why the
    workers must all be co-scheduled; the toy math here (a distributed mean) keeps the example
    dependency-free, but the topology (N co-resident workers) is the honest gang pattern."""
    import random

    shard = [random.Random(rank).uniform(0, 100) for _ in range(n)]
    return Contribution(rank=rank, local_sum=sum(shard), local_count=len(shard))


def _combine(parts: list[Contribution]) -> float:
    """Fan-in: combine the gang members' contributions into the global average. A plain function,
    not a task, so it does NOT join the gang: only the N workers are gang members (cardinality N)."""
    return sum(p.local_sum for p in parts) / sum(p.local_count for p in parts)


@driver.task
async def distributed_average(n: int = 5000) -> float:
    # Fan out EXACTLY `WORKERS` gang members, so the count matches gang_cardinality. asyncio.gather
    # submits them together; Armada co-schedules the whole gang all-or-nothing.
    parts = await asyncio.gather(*(worker(rank=r, n=n) for r in range(WORKERS)))
    return _combine(list(parts))


if __name__ == "__main__":
    from _runner import run

    result: float = run(distributed_average, n=5000)
    print(f"\nglobal average = {result:.2f}  "
          f"(computed by {WORKERS} workers co-scheduled as one Armada gang)")
