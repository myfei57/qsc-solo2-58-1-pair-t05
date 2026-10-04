"""Multi-ship departure batch endpoints."""

from __future__ import annotations

from portpower.console.flows.batch import (
    BatchCreateRequest,
    BatchReorderRequest,
    BatchResumeRequest,
    batch_view,
    create_batch,
    list_batches,
    reorder_batch,
    resume_batch,
)
from portpower.console.http import BadRequest, Request, Response


def batch_create(runtime, request: Request) -> Response:
    payload = request.json_object()
    view = create_batch(runtime, BatchCreateRequest.from_payload(payload))
    return Response.json(200, view)


def batch_resume(runtime, request: Request) -> Response:
    payload = request.json_object()
    view = resume_batch(runtime, BatchResumeRequest.from_payload(payload))
    return Response.json(200, view)


def batch_reorder(runtime, request: Request) -> Response:
    payload = request.json_object()
    view = reorder_batch(runtime, BatchReorderRequest.from_payload(payload))
    return Response.json(200, view)


def batch_status(runtime, request: Request) -> Response:
    batch_id = request.query_one("id")
    if not batch_id:
        raise BadRequest("id is required")
    return Response.json(200, batch_view(runtime, batch_id))


def batch_list(runtime, request: Request) -> Response:
    return Response.json(200, list_batches(runtime))
