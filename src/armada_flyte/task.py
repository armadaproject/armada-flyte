"""Authoring surface: route a Flyte 2 ``@env.task`` to Armada.

Set ``plugin_config=ArmadaConfig(...)`` on a ``TaskEnvironment`` and every ``@env.task`` in it runs
its function body in an Armada pod (via :class:`ArmadaFunctionTask`, registered as the plugin for
:class:`ArmadaConfig`). ``ArmadaConfig`` stays minimal: declare resources the stock-Flyte way
(``flyte.Resources``); ``ArmadaConfig`` carries only the Armada-specific knobs.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Dict, Optional

from flyte.connectors import AsyncConnectorExecutorMixin
from flyte.extend import AsyncFunctionTaskTemplate, TaskPluginRegistry
from flyte.models import SerializationContext


@dataclass
class ArmadaConfig:
    """Armada submission knobs for a TaskEnvironment (serialised into the task template's ``custom``).

    Resources are declared the stock-Flyte way (``flyte.Resources`` on the env or ``@env.task``);
    ``cpu``/``memory`` here are an optional override (the escape hatch).
    """

    queue: str = "flyte"
    job_set_id: str = "flyte-dag"
    namespace: str = "default"
    priority: int = 1
    # Optional resource override; normally resources are declared via flyte.Resources instead.
    cpu: Optional[str] = None
    memory: Optional[str] = None
    # Gang scheduling: tasks sharing a gang_id (all declaring the same gang_cardinality of 2+) are
    # scheduled all-or-nothing together. Leave gang_id None for an ordinary job.
    gang_id: Optional[str] = None
    gang_cardinality: int = 0
    gang_node_uniformity_label: Optional[str] = None


class ArmadaFunctionTask(AsyncConnectorExecutorMixin, AsyncFunctionTaskTemplate):
    """A Flyte ``@env.task`` whose function body runs inside an Armada pod.

    Registered as the plugin for ``ArmadaConfig``, so a whole environment routes to Armada::

        env = flyte.TaskEnvironment("ml", image=img, plugin_config=ArmadaConfig(queue="compute"))

        @env.task
        async def square(x: int) -> int:
            return x * x

    Flyte renders each task into a container (its ``a0`` entrypoint, which loads the code bundle,
    reads inputs from the configured blob store, runs the function, and writes outputs back). The
    connector wraps that container into an Armada job, so the function body executes in the pod.
    """

    _TASK_TYPE = "armada"

    def __post_init__(self):
        super().__post_init__()
        self.task_type = self._TASK_TYPE

    def custom_config(self, sctx: SerializationContext) -> Dict[str, Any]:
        return asdict(self.plugin_config) if self.plugin_config else {}


# Wire ArmadaConfig as a TaskEnvironment plugin_config: any env created with
# plugin_config=ArmadaConfig(...) builds its tasks as ArmadaFunctionTask.
TaskPluginRegistry.register(ArmadaConfig, ArmadaFunctionTask)
