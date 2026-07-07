"""Shared run helper for the examples. Import it INSIDE an example's ``if __name__ == "__main__"``
block only, so the task pod (which imports the example module to load the task, but never runs
__main__) never needs to import this file.

It submits the example through the Flyte backend (via ``flyte.init`` at localhost:30080) and waits
for the typed result. The connector routes the task to Armada. Adjust the endpoint for your backend.
"""

from __future__ import annotations

import flyte
import flyte.remote


def run(entrypoint, **inputs):
    """Submit entrypoint through the Flyte backend, returning its first output."""
    flyte.init(endpoint="localhost:30080", insecure=True, project="flytesnacks", domain="development")
    r = flyte.run(entrypoint, **inputs)
    print(f"\nsubmitted run {r.name}\n  UI: {r.url}")
    r.wait()
    return flyte.remote.Run.get(r.name).outputs()[0]
