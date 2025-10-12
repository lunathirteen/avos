import pytest
from unittest.mock import MagicMock
from avos.services.assignment_service import AssignmentService
from avos.services.splitter import HashBasedSplitter
from avos.models.layer import Layer, LayerSlot
from avos.models.experiment import Experiment


def make_layer(layer_id="layer1", salt="abc", slots=5):
    layer = MagicMock(spec=Layer)
    layer.layer_id = layer_id
    layer.layer_salt = salt
    layer.total_slots = slots
    return layer


def make_slot(layer_id, index, exp_id=None):
    slot = MagicMock(spec=LayerSlot)
    slot.layer_id = layer_id
    slot.slot_index = index
    slot.experiment_id = exp_id
    return slot


def make_experiment(exp_id="exp1", variants=["A", "B"], allocations=[0.5, 0.5], splitter="hash"):
    exp = MagicMock(spec=Experiment)
    exp.experiment_id = exp_id
    exp.splitter_type = splitter
    exp.get_variant_list.return_value = variants
    exp.get_traffic_dict.return_value = dict(zip(variants, allocations))
    exp.is_active.return_value = True
    exp.get_stratum_allocations.return_value = {}
    exp.get_geo_allocations.return_value = {}
    return exp


def test_assignment_for_assigned_slot(monkeypatch):
    """Assigned slot with active experiment returns correct assignment."""
    layer = make_layer()
    slot = make_slot(layer.layer_id, 0, "exp1")
    exp = make_experiment()

    # Fake SQLAlchemy session and query
    session = MagicMock()
    session.execute.return_value.scalar_one_or_none.return_value = slot
    session.get.return_value = exp

    assignment = AssignmentService.assign_for_layer(session, layer, "userX")
    assert assignment["experiment_id"] == "exp1"
    assert assignment["variant"] in ["A", "B"]
    assert assignment["status"] == "assigned"


def test_assignment_for_unassigned_slot():
    """Unassigned slot returns 'not_assigned' and None experiment."""
    layer = make_layer()
    slot = make_slot(layer.layer_id, 0, None)
    session = MagicMock()
    session.execute.return_value.scalar_one_or_none.return_value = slot

    assignment = AssignmentService.assign_for_layer(session, layer, "userY")
    assert assignment["experiment_id"] is None
    assert assignment["variant"] is None
    assert assignment["status"] == "not_assigned"


def test_assignment_for_inactive_experiment():
    """Inactive experiment returns 'experiment_inactive'."""
    layer = make_layer()
    slot = make_slot(layer.layer_id, 0, "exp2")
    exp = make_experiment(exp_id="exp2")
    exp.is_active.return_value = False

    session = MagicMock()
    session.execute.return_value.scalar_one_or_none.return_value = slot
    session.get.return_value = exp

    assignment = AssignmentService.assign_for_layer(session, layer, "userZ")
    assert assignment["experiment_id"] == "exp2"
    assert assignment["variant"] is None
    assert assignment["status"] == "experiment_inactive"


def test_assignment_slot_hash_consistency():
    """User gets consistent slot for same layer and salt."""
    layer = make_layer(salt="testhash", slots=10)
    idx1 = AssignmentService._calculate_user_slot("testhash", 10, "user1")
    idx2 = AssignmentService._calculate_user_slot("testhash", 10, "user1")
    assert idx1 == idx2


def test_bulk_assignment(monkeypatch):
    layer = make_layer()
    slot = make_slot(layer.layer_id, 0, "exp1")
    exp = make_experiment()
    session = MagicMock()
    session.execute.return_value.scalar_one_or_none.return_value = slot
    session.get.return_value = exp

    assign_dict = AssignmentService.assign_bulk_for_layer(session, layer, ["u1", "u2"])
    assert set(assign_dict.keys()) == {"u1", "u2"}
    for assign in assign_dict.values():
        assert assign["experiment_id"] == "exp1"
        assert assign["status"] == "assigned"


def test_preview_assignment_distribution(monkeypatch):
    layer = make_layer()
    slot = make_slot(layer.layer_id, 0, "exp1")
    exp = make_experiment()
    session = MagicMock()
    session.execute.return_value.scalar_one_or_none.return_value = slot
    session.get.return_value = exp
    uids = [f"user{i}" for i in range(50)]

    preview = AssignmentService.preview_assignment_distribution(session, layer, uids)
    assert "total_users" in preview
    assert "assignment_distribution" in preview
    assert preview["unassigned_count"] == 0
    assert preview["assignment_rate"] == 100.0
