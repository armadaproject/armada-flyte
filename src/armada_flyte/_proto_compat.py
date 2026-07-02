"""Resolve a protobuf descriptor-pool clash between armada_client and flyteidl2.

``armada_client`` vendors its own ``armada_client.google.api`` package, whose ``http_pb2`` /
``annotations_pb2`` register the proto file ``google/api/http.proto``. ``flyteidl2`` (pulled in
by ``flyte.connectors``) registers the *standard* ``google.api`` copy of the same file. The
upb descriptor pool rejects the second registration with::

    TypeError: Couldn't build proto file into descriptor pool: duplicate file name google/api/http.proto

Importing this module first imports the standard ``google.api`` protos (registering
``google/api/http.proto`` exactly once), then aliases the vendored module paths to the standard
ones in ``sys.modules``. When ``armada_client.*_pb2`` later does
``from armada_client.google.api import annotations_pb2`` it resolves to the already-registered
standard module, so no duplicate registration occurs.

Import this BEFORE importing anything from ``armada_client``.
"""

import sys

from google.api import annotations_pb2 as _std_annotations_pb2
from google.api import http_pb2 as _std_http_pb2

sys.modules["armada_client.google.api.http_pb2"] = _std_http_pb2
sys.modules["armada_client.google.api.annotations_pb2"] = _std_annotations_pb2
