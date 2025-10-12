import pytest
from avos.services.splitter import (
    HashBasedSplitter,
    RandomSplitter,
    StratifiedSplitter,
    SegmentedSplitter,
    GeoBasedSplitter,
)


def test_hash_based_splitter_deterministic():
    splitter = HashBasedSplitter("exp1")
    variants = ["A", "B"]
    allocs = [0.5, 0.5]
    uid = "user123"
    v1 = splitter.assign_variant(uid, variants, allocs)
    v2 = splitter.assign_variant(uid, variants, allocs)
    assert v1 in variants
    assert v1 == v2  # Deterministic


def test_random_splitter_non_deterministic():
    splitter = RandomSplitter()
    variants = ["A", "B"]
    allocs = [0.5, 0.5]
    uid = "random_user"
    values = {splitter.assign_variant(uid, variants, allocs) for _ in range(10)}
    assert values <= set(variants)
    # At least occasionally different
    assert len(values) > 1


def test_stratified_splitter_per_stratum():
    config = {"female": (["A", "B"], [0.7, 0.3]), "male": (["A", "B"], [0.4, 0.6])}
    splitter = StratifiedSplitter("exp2", config)
    uid = "user100"
    res_f_1 = splitter.assign_variant(uid, None, None, stratum="female")
    res_f_2 = splitter.assign_variant(uid, None, None, stratum="female")
    res_m_1 = splitter.assign_variant(uid, None, None, stratum="male")
    res_m_2 = splitter.assign_variant(uid, None, None, stratum="male")
    # Valid and deterministic
    assert res_f_1 == res_f_2
    assert res_f_1 in ["A", "B"]
    assert res_m_1 == res_m_2
    assert res_m_1 in ["A", "B"]
    # CAN be the same between strata - test only for validity


def test_segmented_splitter_per_segment():
    config = {"segmentA": (["A", "B"], [0.2, 0.8]), "segmentB": (["A", "B"], [0.8, 0.2])}
    splitter = SegmentedSplitter("exp3", config)
    uid = "seguser1"
    res_a = splitter.assign_variant(uid, None, None, segment="segmentA")
    res_b = splitter.assign_variant(uid, None, None, segment="segmentB")
    assert res_a in ["A", "B"]
    assert res_b in ["A", "B"]
    assert res_a != res_b or config["segmentA"] == config["segmentB"]


def test_geo_based_splitter_per_geo():
    config = {"US": (["A", "B"], [0.5, 0.5]), "UK": (["A", "B"], [0.1, 0.9])}
    splitter = GeoBasedSplitter("exp_geo", config)
    uid = "geouser"
    us_variant = splitter.assign_variant(uid, None, None, geo="US")
    uk_variant = splitter.assign_variant(uid, None, None, geo="UK")
    assert us_variant in ["A", "B"]
    assert uk_variant in ["A", "B"]
    assert us_variant != uk_variant or config["US"] == config["UK"]


def test_splitter_invalid_allocations():
    splitter = HashBasedSplitter("exp_bad")
    # Allocations don't sum to 1 (should raise error)
    with pytest.raises(ValueError):
        splitter.assign_variant("badid", ["A", "B"], [0.5, 0.4])


def test_segmented_splitter_invalid_segment():
    config = {"segmentA": (["A", "B"], [0.5, 0.5])}
    splitter = SegmentedSplitter("exp_missing", config)
    with pytest.raises(ValueError):
        splitter.assign_variant("userX", None, None, segment="NonExistent")


def test_geo_based_splitter_invalid_geo():
    config = {"US": (["A", "B"], [0.5, 0.5])}
    splitter = GeoBasedSplitter("exp_missing", config)
    with pytest.raises(ValueError):
        splitter.assign_variant("userY", None, None, geo="RU")
