"""Complex: a small end-to-end ML pipeline on Armada. Typed data through stages plus a fan-out.

    make_data         build a training set and a held-out test set
    cv_fold (alpha x fold) in parallel Armada jobs    k-fold cross-validation over ridge strengths
    pick the best alpha, fit the final model, evaluate on the test set

The cross-validation jobs are INDEPENDENT (no gang): alphas x k parallel Armada jobs. Pure stdlib
(a closed-form 1-D ridge) so it runs with no image changes; swap in numpy/scikit-learn for real
models by adding them to the task image. Run:

    ./demo/run.sh examples/ml_pipeline.py              # default: runs on Armada, shows in the Flyte UI
"""

from __future__ import annotations

import asyncio
import os
import statistics
from dataclasses import dataclass

import flyte
from armada_flyte import ArmadaConfig

IMAGE = os.environ.get("ARMADA_TASK_IMAGE", "armada-flyte-task:v1")

work = flyte.TaskEnvironment(
    name="ml",
    image=IMAGE,
    resources=flyte.Resources(cpu="500m", memory="512Mi"),
    plugin_config=ArmadaConfig(queue="flyte"),
)
# The driver orchestrates the pipeline. It runs as a backend pod, so it needs the same task
# image.
driver = flyte.TaskEnvironment(name="driver", image=IMAGE, depends_on=[work])


@dataclass
class Dataset:
    x: list[float]
    y: list[float]


@dataclass
class FoldResult:
    alpha: float
    val_mse: float


@dataclass
class Model:
    slope: float
    intercept: float
    alpha: float


@dataclass
class Result:
    model: Model
    test_mse: float


def _fit_ridge(x, y, alpha):
    n = len(x)
    mx, my = sum(x) / n, sum(y) / n
    sxx = sum((xi - mx) ** 2 for xi in x)
    sxy = sum((xi - mx) * (yi - my) for xi, yi in zip(x, y))
    slope = sxy / (sxx + alpha)  # ridge-shrunk slope
    return slope, my - slope * mx


def _mse(slope, intercept, x, y):
    return sum((slope * xi + intercept - yi) ** 2 for xi, yi in zip(x, y)) / len(x)


@work.task
async def make_data(n: int, seed: int) -> Dataset:
    import random

    rng = random.Random(seed)
    xs = [rng.uniform(-5, 5) for _ in range(n)]
    ys = [3 * xi + 2 + rng.gauss(0, 1) for xi in xs]  # y = 3x + 2 + noise
    return Dataset(x=xs, y=ys)


@work.task
async def cv_fold(data: Dataset, alpha: float, fold: int, k: int) -> FoldResult:
    val_x, val_y = data.x[fold::k], data.y[fold::k]
    tr_x = [v for i, v in enumerate(data.x) if i % k != fold]
    tr_y = [v for i, v in enumerate(data.y) if i % k != fold]
    slope, intercept = _fit_ridge(tr_x, tr_y, alpha)
    return FoldResult(alpha=alpha, val_mse=_mse(slope, intercept, val_x, val_y))


@work.task
async def fit(data: Dataset, alpha: float) -> Model:
    slope, intercept = _fit_ridge(data.x, data.y, alpha)
    return Model(slope=slope, intercept=intercept, alpha=alpha)


@work.task
async def evaluate(model: Model, test: Dataset) -> float:
    return _mse(model.slope, model.intercept, test.x, test.y)


@driver.task
async def train(n: int = 600, k: int = 3, alphas: tuple[float, ...] = (0.0, 1.0, 10.0)) -> Result:
    full = await make_data(n=n, seed=1)
    test = await make_data(n=n // 4, seed=99)
    folds = await asyncio.gather(
        *(cv_fold(data=full, alpha=a, fold=f, k=k) for a in alphas for f in range(k))
    )
    cv_mse = {a: statistics.fmean(r.val_mse for r in folds if r.alpha == a) for a in alphas}
    best = min(cv_mse, key=cv_mse.get)
    model = await fit(data=full, alpha=best)
    test_mse = await evaluate(model=model, test=test)
    return Result(model=model, test_mse=test_mse)


if __name__ == "__main__":
    from _runner import run

    result: Result = run(train)
    m = result.model
    print(f"\nbest alpha = {m.alpha}  test MSE = {result.test_mse:.3f}  fit y ~ {m.slope:.2f} x + "
          f"{m.intercept:.2f}  (true model: y = 3x + 2, found across parallel Armada pods)")
