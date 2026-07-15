"""First-class Armada gang scheduling for Flyte 2 tasks.

A gang is a set of tasks Armada schedules all-or-nothing: either every member is placed (optionally all
onto nodes sharing one value of a node-uniformity label) or none of them start. This is the primitive
for a distributed job whose workers must run at the same time, an MPI world or an all-reduce ring,
where no worker can make progress while a peer is missing.

Declare membership by adding tasks to a :class:`Gang`, then await :meth:`Gang.run`::

    gang = Gang(node_uniformity_label="kubernetes.io/hostname")
    for rank in range(4):
        gang.add(worker, rank=rank, n=n)
    parts = await gang.run()

The user says only *what belongs to the gang* and *which label they must share*. armada_flyte derives
the gang id and the cardinality from the members added, so the two can never disagree with each other
the way a hand-set ``gang_cardinality`` can drift from the number of tasks actually fanned out.

The cardinality must be stamped on every member before Armada schedules any of them, and it is only
known once all members are collected. So a gang is declared (``add``) and then launched (``run``) rather
than awaited inline: ``run`` is the one moment the count is both known and still ahead of submission.
"""

from __future__ import annotations

import asyncio
import uuid
from typing import Any, List, Tuple

# The connector reads these off each member's container (stamped via task.override(env_vars=...)) to
# build the gang annotations, then strips them from the pod. The prefix stays off Armada's reserved
# ``ARMADA_`` namespace, whose own ``ARMADA_GANG_*`` vars Armada injects into gang pods at runtime.
_GANG_ENV_PREFIX = "ARMADAFLYTE_GANG_"
GANG_ID_ENV = _GANG_ENV_PREFIX + "ID"
GANG_CARDINALITY_ENV = _GANG_ENV_PREFIX + "CARDINALITY"
GANG_NODE_UNIFORMITY_ENV = _GANG_ENV_PREFIX + "NODE_UNIFORMITY_LABEL"


class Gang:
    """A set of Flyte tasks to submit together as one all-or-nothing Armada gang.

    :param node_uniformity_label: if set, Armada places every member on nodes sharing one value of this
        label (e.g. ``kubernetes.io/hostname`` to force them all onto a single node). Left unset, the
        members are still co-scheduled all-or-nothing but may land on different nodes.
    """

    def __init__(self, node_uniformity_label: str | None = None) -> None:
        self.node_uniformity_label = node_uniformity_label
        self._members: List[Tuple[Any, tuple, dict]] = []

    def add(self, task: Any, *args: Any, **kwargs: Any) -> "Gang":
        """Add one task invocation to the gang. Nothing is submitted until :meth:`run`.

        The task and its arguments may differ per member, so a heterogeneous gang (say a coordinator
        plus workers) is just several different tasks added to the same Gang. Every member must be an
        Armada task (its ``TaskEnvironment`` has ``plugin_config=ArmadaConfig(...)``): a non-Armada
        member would run as an ordinary Flyte pod that never reaches the connector, so it would never
        join the gang and the other members would deadlock QUEUED waiting for a peer that never arrives.
        """
        if getattr(task, "task_type", None) != "armada":
            raise ValueError(
                f"gang member {getattr(task, 'name', task)!r} is not an Armada task; give its "
                "TaskEnvironment plugin_config=ArmadaConfig(...) so it routes to Armada"
            )
        self._members.append((task, args, kwargs))
        return self

    async def run(self) -> list:
        """Submit every member together as one gang and return their results in the order added.

        Armada schedules the whole gang or none of it. The gang id is generated here (unique per call,
        so re-running the driver forms a fresh gang) and the cardinality is the number of members added.

        Notes:
        - Members must not belong to a reusable ``TaskEnvironment``: the gang values are injected with
          ``task.override(env_vars=...)``, which Flyte disallows for reusable tasks. Reuse (warm shared
          containers) also does not fit a gang, whose members are fresh co-scheduled pods.
        - If submitting one member fails, the whole call raises, but members already submitted may sit
          queued (Armada holds a gang until all its members arrive) until the run is cleaned up.
        """
        cardinality = len(self._members)
        if cardinality < 2:
            raise ValueError(f"a gang needs at least 2 members; got {cardinality}")

        gang_id = uuid.uuid4().hex
        coros = []
        for task, args, kwargs in self._members:
            # override(env_vars=...) replaces rather than merges, so carry the task's own env forward.
            env_vars = dict(getattr(task, "env_vars", None) or {})
            env_vars[GANG_ID_ENV] = gang_id
            env_vars[GANG_CARDINALITY_ENV] = str(cardinality)
            if self.node_uniformity_label:
                env_vars[GANG_NODE_UNIFORMITY_ENV] = self.node_uniformity_label
            coros.append(task.override(env_vars=env_vars)(*args, **kwargs))
        return await asyncio.gather(*coros)

    @classmethod
    async def map(
        cls,
        task: Any,
        *iterables: Any,
        node_uniformity_label: str | None = None,
        **shared: Any,
    ) -> list:
        """Convenience for the common homogeneous gang: one member per zipped item of ``iterables``.

        Each member is ``task(*values, **shared)`` for the zipped ``values``, so
        ``await Gang.map(worker, range(4), node_uniformity_label="kubernetes.io/hostname", n=n)`` runs a
        4-member gang of ``worker(rank, n=n)``.
        """
        gang = cls(node_uniformity_label=node_uniformity_label)
        for values in zip(*iterables):
            gang.add(task, *values, **shared)
        return await gang.run()
