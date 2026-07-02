"""Simple: one @env.task that runs in an Armada pod. Write normal typed Python, it runs on Armada.

The only Armada-specific line is plugin_config=ArmadaConfig(queue=...). Resources are declared the
stock-Flyte way via flyte.Resources. Run:

    ./demo/run.sh examples/function.py               # default: runs on Armada, shows in the Flyte UI
"""

from __future__ import annotations

import os

import flyte
from armada_flyte import ArmadaConfig

IMAGE = os.environ.get("ARMADA_TASK_IMAGE", "armada-flyte-task:v1")

env = flyte.TaskEnvironment(
    name="quant",
    image=IMAGE,
    resources=flyte.Resources(cpu=1, memory="512Mi"),
    plugin_config=ArmadaConfig(queue="flyte"),
)


@env.task
async def black_scholes_call(spot: float, strike: float, vol: float, rate: float, t: float) -> float:
    """The Black-Scholes price of a European call option."""
    import math

    def cdf(x: float) -> float:
        return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))

    d1 = (math.log(spot / strike) + (rate + 0.5 * vol * vol) * t) / (vol * math.sqrt(t))
    d2 = d1 - vol * math.sqrt(t)
    return spot * cdf(d1) - strike * math.exp(-rate * t) * cdf(d2)


if __name__ == "__main__":
    from _runner import run

    price = run(black_scholes_call, spot=100.0, strike=100.0, vol=0.2, rate=0.05, t=1.0)
    print(f"\ncall price = {price:.4f}  (expected ~10.4506, computed in an Armada pod)")
