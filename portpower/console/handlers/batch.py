"""Ordered, resumable batch disconnect endpoints."""

from __future__ import annotations

from portpower.console.http import BadRequest, Request, Response
from portpower.console.validation import (
    optional_number,
    optional_text,
    require_text,
)
from portpower.disconn.gateway import DisconnectTarget
from portpower.disconn.service import job_view


def _service(runtime):
    return runtime.batch_service


def _job_id(payload: dict[str, object]) -> str:
    return require_text(payload, "id")


def _parse_targets(payload: dict[str, object]) -> list[DisconnectTarget]:
    raw = payload.get("targets")
    if not isinstance(raw, list) or not raw:
        raise BadRequest("targets must be a non-empty list")
    targets: list[DisconnectTarget] = []
    for entry in raw:
        if not isinstance(entry, dict):
            raise BadRequest("each target must be an object")
        key = require_text(entry, "key")
        label = optional_text(entry, "label", key)
        requires_raw = entry.get("requires", [])
        if not isinstance(requires_raw, list) or not all(
            isinstance(value, str) and value.strip()
            for value in requires_raw
        ):
            raise BadRequest("requires must be a list of item keys")
        priority_raw = entry.get("priority", 0)
        if isinstance(priority_raw, bool) or not isinstance(
            priority_raw, int
        ):
            raise BadRequest("priority must be an integer")
        final_value = optional_number(entry, "final_value", 0.0)
        targets.append(
            DisconnectTarget(
                key=key,
                label=label,
                requires=frozenset(value.strip() for value in requires_raw),
                priority=priority_raw,
                final_value=final_value,
            )
        )
    return targets


def batch_create(runtime, request: Request) -> Response:
    payload = request.json_object()
    service = _service(runtime)
    job = service.create_disconnect(_parse_targets(payload))
    job = service.runner.advance(job.job_id)
    return Response.json(201, job_view(job))


def batch_get(runtime, request: Request) -> Response:
    job_id = request.query_one("id")
    if not job_id:
        raise BadRequest("id is required")
    job = _service(runtime).repository.load(job_id)
    return Response.json(200, job_view(job))


def batch_list(runtime, request: Request) -> Response:
    service = _service(runtime)
    jobs = [
        job_view(service.repository.load(job_id))
        for job_id in service.repository.list_ids()
    ]
    return Response.json(200, {"count": len(jobs), "batches": jobs})


def batch_resume(runtime, request: Request) -> Response:
    payload = request.json_object()
    service = _service(runtime)
    job = service.runner.advance(_job_id(payload))
    return Response.json(200, job_view(job))


def batch_retry(runtime, request: Request) -> Response:
    payload = request.json_object()
    service = _service(runtime)
    job = service.runner.retry(_job_id(payload), require_text(payload, "key"))
    return Response.json(200, job_view(job))


def batch_skip(runtime, request: Request) -> Response:
    payload = request.json_object()
    service = _service(runtime)
    note = optional_text(payload, "note", "operator confirmed on site")
    job = service.runner.skip(
        _job_id(payload), require_text(payload, "key"), note
    )
    return Response.json(200, job_view(job))


def batch_order(runtime, request: Request) -> Response:
    payload = request.json_object()
    service = _service(runtime)
    job_id = _job_id(payload)
    requires_raw = payload.get("requires", {})
    priority_raw = payload.get("priority", {})
    if not isinstance(requires_raw, dict) or not isinstance(priority_raw, dict):
        raise BadRequest("requires and priority must be objects")
    requires: dict[str, list[str]] = {}
    for key, values in requires_raw.items():
        if not isinstance(key, str) or not isinstance(values, list) or not all(
            isinstance(value, str) for value in values
        ):
            raise BadRequest("requires maps item keys to lists of keys")
        requires[key] = values
    priority: dict[str, int] = {}
    for key, value in priority_raw.items():
        if isinstance(value, bool) or not isinstance(value, int):
            raise BadRequest("priority maps item keys to integers")
        priority[key] = value
    if not requires and not priority:
        job = service.runner.reorder(job_id)
    else:
        job = service.runner.update_constraints(
            job_id, requires=requires or None, priority=priority or None
        )
    return Response.json(200, job_view(job))
