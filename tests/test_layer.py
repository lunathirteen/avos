from datetime import datetime, timedelta
import pytest
from avos.layer import Slot, Layer


def test_slot_creation():
    slot = Slot(
        slot_id="slot_001",
        traffic_percentage=80.0,
        variants=['a', 'b'],
        weights=[50.0, 50.0]
    )

    assert slot.slot_id == "slot_001"
    assert len(slot.variants) == 2
    assert abs(sum(slot.weights) - 100.0) < 0.01
    assert slot.traffic_percentage == 80.0


def test_layer_creation():
    layer = Layer(
        layer_id="layer_001",
        layer_salt="layer_salt"
    )

    assert layer.layer_id == "layer_001"
    assert layer.total_traffic_percentage == 100.0
    assert layer.slots == []


def test_empty_slot_id():
    with pytest.raises(ValueError, match="slot_id cannot be empty"):
        Slot(
            slot_id="",
            traffic_percentage=80.0,
            variants=['a', 'b'],
            weights=[50.0, 50.0]
        )

def test_slot_traffic_percentage():
    with pytest.raises(ValueError, match="slot traffic_percentage must be in"):
        Slot(
            slot_id="bad_slot",
            traffic_percentage=110.0,
            variants=['a', 'b'],
            weights=[50.0, 50.0]
        )

def test_slot_weights():
    with pytest.raises(ValueError, match="slot weights must sum to 100"):
        Slot(
            slot_id="bad_slot",
            traffic_percentage=100.0,
            variants=['a', 'b'],
            weights=[60.0, 50.0]
        )

def test_slot_zero_weights():
    with pytest.raises(ValueError, match="variant weights must be > 0"):
        Slot(
            slot_id="bad_slot",
            traffic_percentage=100.0,
            variants=['a', 'b'],
            weights=[60.0, 0.0]
        )

def test_slot_different_lengths():
    with pytest.raises(ValueError, match="variants and weights lengths must match"):
        Slot(
            slot_id="bad_slot",
            traffic_percentage=100.0,
            variants=['a', 'b', 'c'],
            weights=[60.0, 30.0]
        )

def test_empty_layer_id():
    with pytest.raises(ValueError, match="layer_id cannot be empty"):
        Layer(
            layer_id="",
            layer_salt="layer_salt"
        )

def test_layer_traffic_percentage():
    with pytest.raises(ValueError, match="layer total_traffic_percentage must be in"):
        Layer(
            layer_id="bad_slot",
            layer_salt="layer_salt",
            total_traffic_percentage=110.0,
        )

def test_layer_slots_pct():
    slot1 = Slot(
        slot_id="slot_001",
        traffic_percentage=80.0,
        variants=['a', 'b'],
        weights=[50.0, 50.0]
    )

    slot2 = Slot(
        slot_id="slot_002",
        traffic_percentage=80.0,
        variants=['a', 'b'],
        weights=[50.0, 50.0]
    )

    with pytest.raises(ValueError, match="sum of slots traffic_percentage must be <= 100"):
        Layer(
            layer_id="bad_slot",
            layer_salt="layer_salt",
            total_traffic_percentage=100.0,
            slots=[slot1, slot2]
        )

def test_layer_add_slot():
    layer_100 = Layer(
        layer_id="layer_001",
        layer_salt="layer_salt"
    )

    slot_80 = Slot(
        slot_id="slot_80",
        traffic_percentage=80.0,
        variants=['a', 'b'],
        weights=[50.0, 50.0]
    )

    layer_100.add_slot(slot_80)
    assert layer_100.get_slot_by_id("slot_80") is not None
    assert layer_100.get_allocated_traffic() == 80
    assert layer_100.get_unallocated_traffic() == 20
    assert layer_100.has_space_for(20) is True

    slot_20 = Slot(
        slot_id="slot_20",
        traffic_percentage=20.0,
        variants=['a', 'b'],
        weights=[50.0, 50.0]
    )

    layer_100.add_slot(slot_20)
    assert layer_100.get_slot_by_id("slot_20") is not None
    assert layer_100.get_allocated_traffic() == 100
    assert layer_100.get_unallocated_traffic() == 0
    assert layer_100.has_space_for(20) is False

    summary = layer_100.get_slots_summary()
    assert len(summary) == 2

    layer_100.clear_all_slots()
    assert layer_100.slots == []

def test_layer_add_existed_slot():
    slot1 = Slot(
        slot_id="slot_001",
        traffic_percentage=80.0,
        variants=['a', 'b'],
        weights=[50.0, 50.0]
    )

    slot2 = Slot(
        slot_id="slot_001",
        traffic_percentage=80.0,
        variants=['a', 'b'],
        weights=[50.0, 50.0]
    )

    with pytest.raises(ValueError, match="Slot with id"):
        layer = Layer(
            layer_id="existed_slot",
            layer_salt="layer_salt",
            slots=[slot1]
        )

        layer.add_slot(slot2)

def test_layer_not_enough_space():
    slot1 = Slot(
        slot_id="slot_001",
        traffic_percentage=80.0,
        variants=['a', 'b'],
        weights=[50.0, 50.0]
    )

    slot2 = Slot(
        slot_id="slot_002",
        traffic_percentage=80.0,
        variants=['a', 'b'],
        weights=[50.0, 50.0]
    )

    layer = Layer(
        layer_id="not_enough_space",
        layer_salt="layer_salt",
        slots=[slot1]
    )

    assert layer.add_slot(slot2) is False

def test_layer_remove_slot():
    layer = Layer(
        layer_id="layer_001",
        layer_salt="layer_salt"
    )

    slot_1 = Slot(
        slot_id="slot_001",
        traffic_percentage=70.0,
        variants=['a', 'b'],
        weights=[50.0, 50.0]
    )

    slot_2 = Slot(
        slot_id="slot_002",
        traffic_percentage=10.0,
        variants=['a', 'b'],
        weights=[50.0, 50.0]
    )

    layer.add_slot(slot_1)
    layer.add_slot(slot_2)

    assert layer.get_slot_by_id("slot_001") is not None
    assert layer.get_slot_by_id("slot_002") is not None
    assert layer.get_allocated_traffic() == 80

    layer.remove_slot("slot_002")

    assert layer.get_slot_by_id("slot_001") is not None
    assert layer.get_slot_by_id("slot_002") is None
    assert layer.get_allocated_traffic() == 70
