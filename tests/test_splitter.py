import pytest
from avos.splitter import HashBasedSplitter, SplitterContext

split_strategy = HashBasedSplitter(experiment_id="exp_1")

splitter = SplitterContext(strategy=split_strategy)


def test_constant_assignment():

    variant_1 = splitter.assign(user_id="user_12345", variants=["a", "b"], weights=[50.0, 50.0])
    variant_2 = splitter.assign(user_id="user_12345", variants=["a", "b"], weights=[50.0, 50.0])
    assert variant_1 == variant_2


def test_empty_user_id():
    with pytest.raises(ValueError, match="user_id cannot be empty"):
        splitter.assign(user_id="", variants=["a", "b"], weights=[50.0, 50.0])


def test_different_lengths():
    with pytest.raises(ValueError, match="variants and weights lists must be of the same length"):
        splitter.assign(user_id="user_12345", variants=["a", "b", "c"], weights=[50.0, 50.0])


def test_zero_weight():
    with pytest.raises(ValueError, match="weight must be greater than 0"):
        splitter.assign(user_id="user_12345", variants=["a", "b"], weights=[50.0, 0.0])


def test_weight_validation():
    with pytest.raises(ValueError, match="weights must sum to 100"):
        splitter.assign(user_id="user_12345", variants=["a", "b"], weights=[50.0, 30.0])
