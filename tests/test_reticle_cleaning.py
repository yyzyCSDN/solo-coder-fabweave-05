from __future__ import annotations

import pytest

from fabweave.core.events import EventLog
from fabweave.core.timebox import TimeBox
from fabweave.reticle.cleaning import (
    CleaningStage,
    InspectionResult,
    cleaning_state_at,
    cleaning_trigger_decision,
    current_cleaning_state,
    record_inspection,
    record_release,
    record_return_to_vendor,
    record_send_for_cleaning,
    reservation_gate,
)
from fabweave.reticle.reservation import Reservation, ReservationBook
from fabweave.workflows.control import reserve_with_cleaning_gate


def _full_cleaning_cycle(log: EventLog, reticle_id: str = "RET-1") -> None:
    """Record a complete cleaning cycle: 送洗 → 返厂 → 检验 → 放行."""
    record_send_for_cleaning(log, reticle_id, "EVT-SEND", 10, "exposure_limit_reached", exposure_count=1000, exposure_limit=1000)
    record_return_to_vendor(log, reticle_id, "EVT-RETURN", 20, "vendor_cleaning")
    record_inspection(log, reticle_id, "EVT-INSPECT", 30, InspectionResult.PASSED, {"particles": 0})
    record_release(log, reticle_id, "EVT-RELEASE", 40)


def test_trigger_decision_fires_on_exposure_limit_and_contamination():
    assert not cleaning_trigger_decision(1000, 1000, False).allowed
    assert not cleaning_trigger_decision(500, 1000, True).allowed
    decision = cleaning_trigger_decision(1000, 1000, True)
    assert "exposure_limit_reached" in decision.reasons
    assert "contamination_detected" in decision.reasons
    assert cleaning_trigger_decision(500, 1000, False).allowed


def test_cleaning_cycle_advances_stages_in_order():
    log = EventLog()
    record_send_for_cleaning(log, "RET-1", "EVT-1", 10, "exposure_limit_reached")
    assert current_cleaning_state(log, "RET-1").stage == CleaningStage.CLEANING
    record_return_to_vendor(log, "RET-1", "EVT-2", 20, "vendor_cleaning")
    assert current_cleaning_state(log, "RET-1").stage == CleaningStage.VENDOR
    record_inspection(log, "RET-1", "EVT-3", 30, InspectionResult.PASSED)
    assert current_cleaning_state(log, "RET-1").stage == CleaningStage.INSPECTION
    record_release(log, "RET-1", "EVT-4", 40)
    assert current_cleaning_state(log, "RET-1").stage == CleaningStage.IN_SERVICE


def test_out_of_order_events_are_rejected():
    log = EventLog()
    with pytest.raises(ValueError):
        record_return_to_vendor(log, "RET-1", "EVT-X", 10, "vendor_cleaning")
    with pytest.raises(ValueError):
        record_inspection(log, "RET-1", "EVT-X", 10, InspectionResult.PASSED)
    with pytest.raises(ValueError):
        record_release(log, "RET-1", "EVT-X", 10)
    record_send_for_cleaning(log, "RET-1", "EVT-1", 10, "exposure_limit_reached")
    with pytest.raises(ValueError):
        record_inspection(log, "RET-1", "EVT-X", 20, InspectionResult.PASSED)
    with pytest.raises(ValueError):
        record_release(log, "RET-1", "EVT-X", 20)
    record_return_to_vendor(log, "RET-1", "EVT-2", 30, "vendor_cleaning")
    with pytest.raises(ValueError):
        record_release(log, "RET-1", "EVT-X", 40)


def test_duplicate_events_are_idempotent():
    log = EventLog()
    first = record_send_for_cleaning(log, "RET-1", "EVT-1", 10, "exposure_limit_reached")
    replay = record_send_for_cleaning(log, "RET-1", "EVT-1", 10, "exposure_limit_reached")
    assert first == replay
    assert current_cleaning_state(log, "RET-1").stage == CleaningStage.CLEANING


def test_failed_inspection_blocks_release():
    log = EventLog()
    record_send_for_cleaning(log, "RET-1", "EVT-1", 10, "exposure_limit_reached")
    record_return_to_vendor(log, "RET-1", "EVT-2", 20, "vendor_cleaning")
    record_inspection(log, "RET-1", "EVT-3", 30, InspectionResult.FAILED, {"particles": 5})
    with pytest.raises(ValueError):
        record_release(log, "RET-1", "EVT-4", 40)
    assert current_cleaning_state(log, "RET-1").stage == CleaningStage.INSPECTION


def test_reservation_gate_blocks_during_cleaning():
    log = EventLog()
    assert reservation_gate(log, "RET-1", 15).allowed
    record_send_for_cleaning(log, "RET-1", "EVT-1", 10, "exposure_limit_reached")
    assert not reservation_gate(log, "RET-1", 15).allowed
    assert reservation_gate(log, "RET-1", 5).allowed
    _full_cleaning_cycle(log, "RET-1")
    assert reservation_gate(log, "RET-1", 45).allowed


def test_reserve_with_cleaning_gate_is_atomic():
    log = EventLog()
    book = ReservationBook()
    record_send_for_cleaning(log, "RET-1", "EVT-1", 10, "exposure_limit_reached")
    requested = [
        Reservation("RET-1", "JOB-1", TimeBox(20, 30), 1),
        Reservation("RET-2", "JOB-2", TimeBox(20, 30), 1),
    ]
    decision = reserve_with_cleaning_gate(log, book, "REQ-1", requested, 25)
    assert not decision.allowed
    assert book.reservations("RET-1") == ()
    assert book.reservations("RET-2") == ()


def test_reserve_with_cleaning_gate_allows_after_release():
    log = EventLog()
    book = ReservationBook()
    _full_cleaning_cycle(log, "RET-1")
    requested = [Reservation("RET-1", "JOB-1", TimeBox(50, 60), 1)]
    decision = reserve_with_cleaning_gate(log, book, "REQ-1", requested, 55)
    assert decision.allowed
    assert len(book.reservations("RET-1")) == 1


def test_historical_state_returns_basis_at_that_time():
    log = EventLog()
    record_send_for_cleaning(log, "RET-1", "EVT-1", 10, "exposure_limit_reached", exposure_count=1000, exposure_limit=1000)
    record_return_to_vendor(log, "RET-1", "EVT-2", 20, "vendor_cleaning", vendor="ACME")
    record_inspection(log, "RET-1", "EVT-3", 30, InspectionResult.PASSED, {"particles": 0})
    record_release(log, "RET-1", "EVT-4", 40)

    at_cleaning = cleaning_state_at(log, "RET-1", 15)
    assert at_cleaning.stage == CleaningStage.CLEANING
    assert at_cleaning.basis["exposure_count"] == 1000
    assert at_cleaning.basis["exposure_limit"] == 1000

    at_vendor = cleaning_state_at(log, "RET-1", 25)
    assert at_vendor.stage == CleaningStage.VENDOR
    assert at_vendor.basis["vendor"] == "ACME"

    at_inspection = cleaning_state_at(log, "RET-1", 35)
    assert at_inspection.stage == CleaningStage.INSPECTION
    assert at_inspection.inspection_result == InspectionResult.PASSED
    assert at_inspection.basis["measurements"] == {"particles": 0}

    after_release = cleaning_state_at(log, "RET-1", 45)
    assert after_release.stage == CleaningStage.IN_SERVICE
    assert not after_release.active


def test_historical_state_before_any_event_is_in_service():
    log = EventLog()
    state = cleaning_state_at(log, "RET-1", 100)
    assert state.stage == CleaningStage.IN_SERVICE
    assert not state.active
    assert state.basis == {}


def test_second_cleaning_cycle_starts_after_release():
    log = EventLog()
    _full_cleaning_cycle(log, "RET-1")
    assert current_cleaning_state(log, "RET-1").stage == CleaningStage.IN_SERVICE
    record_send_for_cleaning(log, "RET-1", "EVT-5", 50, "contamination_detected", contaminated=True)
    assert current_cleaning_state(log, "RET-1").stage == CleaningStage.CLEANING
    assert current_cleaning_state(log, "RET-1").basis["contaminated"] is True
