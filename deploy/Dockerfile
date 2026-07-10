# The Armada connector as a standalone gRPC service image.
#
# The connector is Flyte's connector-runtime `c0` plus this package's `flyte.connectors` entry point,
# which registers the `armada` connector on import. A Flyte backend routes `armada` tasks to it.
# Configure it at runtime with ARMADA_URL and the FLYTE_BLOB_* vars (see deploy/README.md).
FROM python:3.11-slim

WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
# Pin flyte to the version the rest of the stack is validated against.
RUN pip install --no-cache-dir "flyte[connector]==2.5.8" .

EXPOSE 8000
CMD ["c0", "--port", "8000"]
