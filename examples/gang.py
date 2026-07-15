"""GANG scheduling (all-or-nothing co-scheduling of a distributed job).

    seed shards            give each worker a slice of a shared dataset
    worker (x N) ONE GANG  N co-dependent workers, scheduled all-or-nothing by Armada
    reduce                 combine the workers' contributions

The N workers form one armada_flyte.Gang, so Armada places ALL N pods together or none. This is the
primitive for a distributed job whose workers must run at the same time (an all-reduce ring, an MPI
world), where a worker cannot progress while its peers are absent. Contrast fanout.py, whose shards
are INDEPENDENT and use no gang: gangs are for co-dependent workers, not parallel busywork.

    ./.venv/bin/python examples/gang.py
"""

from __future__ import annotations

import os
from dataclasses import dataclass

import flyte
from armada_flyte import ArmadaConfig, Gang

IMAGE = os.environ.get("ARMADA_TASK_IMAGE", "armada-flyte-task:v1")

# Number of workers in the gang. Gang derives the cardinality from the members, so this is the only
# place the size is written. Raise it past the node's capacity to watch the whole gang stay QUEUED.
WORKERS = 4

# An ordinary Armada env. The gang is expressed at the fan-out below, not here.
work = flyte.TaskEnvironment(
    name="gang",
    image=IMAGE,
    # Sized so the whole gang (4 x 256Mi) fits the devbox node at once.
    resources=flyte.Resources(cpu="500m", memory="256Mi"),
    plugin_config=ArmadaConfig(queue="flyte"),
)
# The driver orchestrates the gang from a backend pod (needs the same image) and is not a gang member.
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
    # Gang.map runs one worker per item as a single all-or-nothing gang (dag.py shows the lower-level
    # Gang().add()/run() form). The cardinality is derived from the members, so it matches the fan-out.
    parts = await Gang.map(worker, range(WORKERS), n=n)
    return _combine(list(parts))


if __name__ == "__main__":
    from _runner import run

    result: float = run(distributed_average, n=5000)
    print(f"\nglobal average = {result:.2f}  "
          f"(computed by {WORKERS} workers co-scheduled as one Armada gang)")
