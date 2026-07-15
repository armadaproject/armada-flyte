# The Armada connector as a standalone gRPC service image.
#
# The connector is Flyte's connector-runtime `c0` plus this package's `flyte.connectors` entry point,
# which registers the `armada` connector on import. A Flyte backend routes `armada` tasks to it.
# Configure it at runtime with ARMADA_URL and the FLYTE_BLOB_* vars (see deploy/README.md).
FROM python:3.13-slim

WORKDIR /app
# Install the heavy dependencies first, in their own layer, so editing the source below does not
# re-download them. flyte is pinned to the version the rest of the stack is validated against.
RUN pip install --no-cache-dir "flyte[connector]==2.5.8" "armada-client>=0.5.0"
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir --no-deps .

RUN useradd --create-home --uid 10001 app
USER app

EXPOSE 8000
CMD ["c0", "--port", "8000"]
