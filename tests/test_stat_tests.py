import math
import pytest

from avos.stat_tests import proportion_difference_test


def test_proportion_difference_invalid_nobs():
    with pytest.raises(ValueError, match="nobs must be positive"):
        proportion_difference_test(1, 0, 1, 10)


def test_proportion_difference_zero_baseline_lift():
    result = proportion_difference_test(0, 10, 1, 10)
    assert math.isinf(result["lift"])
