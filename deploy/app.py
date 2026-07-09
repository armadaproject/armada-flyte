"""Deploy the Armada connector as a Flyte connector service.

A deployed Flyte backend routes `armada` tasks to this gRPC service, so workflows can run against
a real backend instead of only under local execution. The same service runs locally with:

    c0 --modules armada_flyte.connector

armada-flyte declares a `flyte.connectors` entry point, so an image that pip-installs it registers
the connector automatically (no `--modules` needed). Deploy with `flyte.deploy(connector)` against
a Flyte backend (`flyte.init_from_config()` must point at one).
"""

from __future__ import annotations

import flyte
import flyte.app

image = flyte.Image.from_debian_base(python_version=(3, 11)).with_pip_packages(
    "flyte[connector]",
    "armada-flyte @ git+https://github.com/armadaproject/armada-flyte.git",
)

connector = flyte.app.ConnectorEnvironment(
    name="armada-connector",
    image=image,
    resources=flyte.Resources(cpu="1", memory="1Gi"),
    env_vars={
        # This is the connector's config for the backend path: the connector runs in this pod, so
        # its settings (the Armada endpoint, the blob store, and any future auth) are set here, on
        # the deployment, not in task code. Prefer a mounted secret over a literal for credentials.
        "ARMADA_URL": "armada-server:50051",
    },
)

if __name__ == "__main__":
    flyte.init_from_config()
    deployment = flyte.deploy(connector)
    print(deployment[0])
