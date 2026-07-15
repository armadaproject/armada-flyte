"""Shared run helper for the examples. Import it INSIDE an example's ``if __name__ == "__main__"``
block only, so the task pod (which imports the example module to load the task, but never runs
__main__) never needs to import this file.

It submits the example through the Flyte backend (``flyte.init`` at ``$FLYTE_ENDPOINT``, default
``localhost:30080``, where the demo's kind NodePort mapping exposes the API) and waits for the typed result. The
connector routes the task to Armada. The example prints the result, and this helper also prints a
link to the run in the Flyte UI.
"""

from __future__ import annotations

import os

import flyte
import flyte.remote

PROJECT = "flytesnacks"
DOMAIN = "development"

# The demo serves the Flyte console at http://localhost:5001/v2 and Armada's Lookout at
# http://localhost:30000. Override FLYTE_UI_BASE / ARMADA_LOOKOUT for a backend that is not the demo.
UI_BASE = os.environ.get("FLYTE_UI_BASE", "http://localhost:5001/v2").rstrip("/")
LOOKOUT = os.environ.get("ARMADA_LOOKOUT", "http://localhost:30000")


def run(entrypoint, **inputs):
    """Submit entrypoint through the Flyte backend, returning its first output."""
    endpoint = os.environ.get("FLYTE_ENDPOINT", "localhost:30080")
    flyte.init(endpoint=endpoint, insecure=True, project=PROJECT, domain=DOMAIN)
    r = flyte.run(entrypoint, **inputs)
    url = f"{UI_BASE}/domain/{DOMAIN}/project/{PROJECT}/runs/{r.name}"
    print(f"\nsubmitted run {r.name}\n  Flyte console: {url}\n  Armada Lookout: {LOOKOUT}")
    r.wait()
    return flyte.remote.Run.get(r.name).outputs()[0]
