"""Reticle cleaning hold state machine.

A reticle that reaches its exposure limit or is found contaminated is
automatically placed on a cleaning hold.  The hold records four stages in
strict order -- send-for-cleaning (送洗), return-to-vendor (返厂), inspection
(检验) and release (放行) -- and blocks reservations while active.

The state is derived from an append-only event stream, so the state and its
basis at any historical instant are queryable.  Duplicate events are
idempotent and out-of-order events are rejected without advancing the state.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any

from fabweave.core.decision import Decision, allow, deny
from fabweave.core.events import EventLog


class CleaningStage(str, Enum):
    IN_SERVICE = "in_service"
    CLEANING = "cleaning"
    VENDOR = "vendor"
    INSPECTION = "inspection"


class InspectionResult(str, Enum):
    PASSED = "passed"
    FAILED = "failed"


EVENT_SENT_FOR_CLEANING = "reticle_sent_for_cleaning"
EVENT_RETURNED_TO_VENDOR = "reticle_returned_to_vendor"
EVENT_INSPECTED = "reticle_inspected"
EVENT_RELEASED = "reticle_released"

_STREAM_PREFIX = "reticle-cleaning:"


def _stream(reticle_id: str) -> str:
    return f"{_STREAM_PREFIX}{reticle_id}"


@dataclass(frozen=True, slots=True)
class CleaningSnapshot:
    """The derived state of a reticle cleaning hold at a point in time."""

    reticle_id: str
    stage: CleaningStage
    active: bool
    inspection_result: InspectionResult | None
    basis: dict[str, Any]
    last_event_at: int | None


def _replay(log: EventLog, reticle_id: str, at: int | None = None) -> CleaningSnapshot:
    stage = CleaningStage.IN_SERVICE
    inspection_result: InspectionResult | None = None
    basis: dict[str, Any] = {}
    last_event_at: int | None = None
    active = False

    for event in log.read(_stream(reticle_id)):
        if at is not None and event.occurred_at > at:
            break
        last_event_at = event.occurred_at
        if event.kind == EVENT_SENT_FOR_CLEANING:
            stage = CleaningStage.CLEANING
            active = True
            basis.update(event.data)
        elif event.kind == EVENT_RETURNED_TO_VENDOR:
            stage = CleaningStage.VENDOR
            basis.update(event.data)
        elif event.kind == EVENT_INSPECTED:
            stage = CleaningStage.INSPECTION
            inspection_result = InspectionResult(event.data["result"])
            basis.update(event.data)
        elif event.kind == EVENT_RELEASED:
            stage = CleaningStage.IN_SERVICE
            active = False
            inspection_result = None
            basis.update(event.data)

    return CleaningSnapshot(
        reticle_id, stage, active, inspection_result, dict(basis), last_event_at
    )


def _sequence(log: EventLog, reticle_id: str) -> int:
    return len(log.read(_stream(reticle_id)))


def _find_event(log: EventLog, reticle_id: str, event_id: str):
    for event in log.read(_stream(reticle_id)):
        if event.event_id == event_id:
            return event
    return None


def cleaning_trigger_decision(
    exposure_count: int, exposure_limit: int, contaminated: bool
) -> Decision:
    """Decide whether a reticle must be placed on cleaning hold.

    Returns ``deny`` (blocked from use) when the exposure limit is reached or
    contamination is detected, ``allow`` otherwise.
    """
    reasons: list[str] = []
    if exposure_count >= exposure_limit:
        reasons.append("exposure_limit_reached")
    if contaminated:
        reasons.append("contamination_detected")
    evidence = {
        "exposure_count": exposure_count,
        "exposure_limit": exposure_limit,
        "contaminated": contaminated,
    }
    return deny(*reasons, **evidence) if reasons else allow(**evidence)


def record_send_for_cleaning(
    log: EventLog,
    reticle_id: str,
    event_id: str,
    at: int,
    reason: str,
    **extra: Any,
):
    """Record the send-for-cleaning stage (送洗).

    This is the automatic entry into the cleaning hold.  It is only valid when
    the reticle is currently in service.
    """
    data = {"reason": reason, **extra}
    state = _replay(log, reticle_id)
    if state.active:
        if _find_event(log, reticle_id, event_id) is not None:
            return log.append(
                _stream(reticle_id),
                _sequence(log, reticle_id),
                event_id,
                at,
                EVENT_SENT_FOR_CLEANING,
                data,
            )
        raise ValueError("reticle is already in cleaning hold")
    return log.append(
        _stream(reticle_id),
        _sequence(log, reticle_id),
        event_id,
        at,
        EVENT_SENT_FOR_CLEANING,
        data,
    )


def record_return_to_vendor(
    log: EventLog,
    reticle_id: str,
    event_id: str,
    at: int,
    reason: str,
    **extra: Any,
):
    """Record the return-to-vendor stage (返厂).

    Only valid after send-for-cleaning.
    """
    data = {"reason": reason, **extra}
    state = _replay(log, reticle_id)
    if state.stage != CleaningStage.CLEANING:
        if _find_event(log, reticle_id, event_id) is not None:
            return log.append(
                _stream(reticle_id),
                _sequence(log, reticle_id),
                event_id,
                at,
                EVENT_RETURNED_TO_VENDOR,
                data,
            )
        raise ValueError("reticle must be sent for cleaning before return to vendor")
    return log.append(
        _stream(reticle_id),
        _sequence(log, reticle_id),
        event_id,
        at,
        EVENT_RETURNED_TO_VENDOR,
        data,
    )


def record_inspection(
    log: EventLog,
    reticle_id: str,
    event_id: str,
    at: int,
    result: InspectionResult,
    measurements: dict[str, Any] | None = None,
    **extra: Any,
):
    """Record the inspection stage (检验).

    Only valid after return-to-vendor.  The inspection result determines
    whether release is permitted.
    """
    data = {"result": result.value, "measurements": measurements or {}, **extra}
    state = _replay(log, reticle_id)
    if state.stage != CleaningStage.VENDOR:
        if _find_event(log, reticle_id, event_id) is not None:
            return log.append(
                _stream(reticle_id),
                _sequence(log, reticle_id),
                event_id,
                at,
                EVENT_INSPECTED,
                data,
            )
        raise ValueError("reticle must be returned to vendor before inspection")
    return log.append(
        _stream(reticle_id),
        _sequence(log, reticle_id),
        event_id,
        at,
        EVENT_INSPECTED,
        data,
    )


def record_release(
    log: EventLog,
    reticle_id: str,
    event_id: str,
    at: int,
    **extra: Any,
):
    """Record the release stage (放行).

    Only valid after inspection with a passed result.  A failed inspection
    blocks release.
    """
    data = {**extra}
    state = _replay(log, reticle_id)
    if state.stage != CleaningStage.INSPECTION:
        if _find_event(log, reticle_id, event_id) is not None:
            return log.append(
                _stream(reticle_id),
                _sequence(log, reticle_id),
                event_id,
                at,
                EVENT_RELEASED,
                data,
            )
        raise ValueError("reticle must be inspected before release")
    if state.inspection_result != InspectionResult.PASSED:
        raise ValueError("only a passed inspection can release the reticle")
    return log.append(
        _stream(reticle_id),
        _sequence(log, reticle_id),
        event_id,
        at,
        EVENT_RELEASED,
        data,
    )


def reservation_gate(log: EventLog, reticle_id: str, at: int) -> Decision:
    """Block reservations while a reticle is on cleaning hold.

    Returns ``deny`` when the cleaning hold is active at ``at``, ``allow``
    otherwise.
    """
    state = _replay(log, reticle_id, at)
    if state.active:
        return deny(
            "reticle_cleaning_hold",
            stage=state.stage.value,
            basis=state.basis,
        )
    return allow(stage=state.stage.value)


def cleaning_state_at(log: EventLog, reticle_id: str, at: int) -> CleaningSnapshot:
    """Return the cleaning state and its basis at a historical instant."""
    return _replay(log, reticle_id, at)


def current_cleaning_state(log: EventLog, reticle_id: str) -> CleaningSnapshot:
    """Return the current cleaning state and its basis."""
    return _replay(log, reticle_id)
