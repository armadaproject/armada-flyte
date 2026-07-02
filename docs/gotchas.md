# Gotchas

The non-obvious things that cost time when wiring Flyte 2 to Armada. Each was hit and fixed
while building this.

## armada_client and Flyte both ship google/api protos

`armada_client` vendors its own `armada_client.google.api`, and Flyte's `flyteidl2` registers
the standard `google.api`. Both describe `google/api/http.proto`, and the second registration
fails with:

```
TypeError: Couldn't build proto file into descriptor pool: duplicate file name google/api/http.proto
```

`_proto_compat.py` fixes this by aliasing the vendored modules to the standard ones in
`sys.modules` before `armada_client` is imported. It is imported first in the package
`__init__`, so importing `armada_flyte` before `armada_client` is enough.

## Use an arm64 Python on Apple Silicon

Flyte depends on `obstore`, which has a native extension. An x86_64 interpreter (for example a
Rosetta build under `/usr/local`) fails to load the arm64 wheel:

```
incompatible architecture (have 'arm64', need 'x86_64')
```

Use a native arm64 interpreter, such as Homebrew's `python3.11`.

## k8s proto fields are camelCase

`armada_client`'s Kubernetes protos keep the original field names, so it is `restartPolicy` and
`terminationGracePeriodSeconds`, not the snake_case you might expect from generated Python
protos.

## Task pod fails with a 404 on its code bundle after recreating the devbox

If you recreate the devbox with a fresh storage volume (a clean blob store) but keep your client-side
bundle cache, `flyte.run` logs `Code bundle found in cache, skipping upload` and the task pod then
fails fetching `.../fast<hash>.tar.gz` with a 404. The client cached the upload against the old store
and skips re-uploading it to the new one. Clear the cache and rerun:

```
rm -f ~/.flyte/local-cache/cache.db
```
