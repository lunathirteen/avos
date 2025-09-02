import pytest
import numpy as np
from avos.srm_tester import SRMTester


# Fixtures for common test setup
@pytest.fixture
def srm_tester():
    """Standard SRM tester with default alpha"""
    return SRMTester(alpha=0.05)


@pytest.fixture
def strict_srm_tester():
    """Strict SRM tester with lower alpha threshold"""
    return SRMTester(alpha=0.01)


class TestBaseSRMTester:
    """Basic functionality tests"""

    def test_equal_distribution(self, srm_tester):
        """Test perfectly balanced assignment - should show no SRM"""
        observed = [100, 100]
        expected = [0.5, 0.5]
        result = srm_tester.test(observed, expected)

        assert not result.reject_null
        assert result.severity == ""
        assert abs(result.p_value - 1.0) < 0.01

    def test_srm_detected(self, srm_tester):
        """Test moderate imbalance that should trigger SRM"""
        observed = [120, 80]
        expected = [0.5, 0.5]
        result = srm_tester.test(observed, expected)

        assert result.reject_null
        assert result.severity in ["*", "**", "***"]

    def test_slight_imbalance_no_srm(self, srm_tester):
        """Test slight imbalance that shouldn't trigger SRM"""
        observed = [505, 495]
        expected = [0.5, 0.5]
        result = srm_tester.test(observed, expected)

        assert not result.reject_null
        assert result.severity == ""
        assert result.p_value > 0.1

    def test_severe_imbalance_high_significance(self, srm_tester):
        """Test severe imbalance with high significance"""
        observed = [700, 300]  # 70/30 split
        expected = [0.5, 0.5]
        result = srm_tester.test(observed, expected)

        assert result.reject_null
        assert result.severity in ["**", "***"]
        assert result.p_value < 0.01


class TestMultiSRMTester:
    """Multi-group tests"""

    def test_three_groups_equal_split(self, srm_tester):
        """Test three-group experiment with equal split"""
        observed = [333, 333, 334]
        expected = [1 / 3, 1 / 3, 1 / 3]
        result = srm_tester.test(observed, expected)

        assert not result.reject_null
        assert result.degrees_of_freedom == 2
        assert len(result.observed_counts) == 3
        assert len(result.expected_counts) == 3

    def test_three_groups_unequal_allocation(self, srm_tester):
        """Test three-group experiment with unequal target allocation"""
        observed = [500, 300, 200]  # Should match 50/30/20 split
        expected = [0.5, 0.3, 0.2]
        result = srm_tester.test(observed, expected)

        assert not result.reject_null
        assert result.total_sample_size == 1000

    def test_three_groups_srm_detected(self, srm_tester):
        """Test three-group experiment with SRM"""
        observed = [600, 300, 100]  # Deviation from equal split
        expected = [1 / 3, 1 / 3, 1 / 3]
        result = srm_tester.test(observed, expected)

        assert result.reject_null
        assert result.severity in ["*", "**", "***"]


class TestValidationSRMTester:
    """Input validation tests"""

    def test_invalid_observed_length(self, srm_tester):
        """Test error when only one group provided"""
        with pytest.raises(ValueError, match="Need at least 2 groups"):
            srm_tester.test([100])

    def test_empty_groups_error(self, srm_tester):
        """Test error when empty list provided"""
        with pytest.raises(ValueError, match="Need at least 2 groups"):
            srm_tester.test([])

    def test_negative_observed_counts(self, srm_tester):
        """Test error when negative counts provided"""
        with pytest.raises(ValueError, match="must be non-negative"):
            srm_tester.test([100, -50])

    def test_mismatched_proportions_error(self, srm_tester):
        """Test error when proportions don't match group count"""
        with pytest.raises(ValueError, match="must match number of groups"):
            srm_tester.test([100, 100], [0.5, 0.3, 0.2])

    def test_zero_counts_handled(self, srm_tester):
        """Test that zero counts are handled properly"""
        observed = [0, 100]
        result = srm_tester.test(observed)

        assert result.reject_null  # Should detect severe imbalance
        assert result.total_sample_size == 100


class TestDefaultBehaviorSRMTester:
    """Default behavior tests"""

    def test_default_expected_proportions(self, srm_tester):
        """Test using default equal proportions when none provided"""
        observed = [50, 50]
        result = srm_tester.test(observed)

        assert not result.reject_null
        assert result.severity == ""

    def test_default_expected_proportions_four_groups(self, srm_tester):
        """Test default proportions with four groups"""
        observed = [250, 250, 250, 250]
        result = srm_tester.test(observed)

        assert not result.reject_null
        assert result.expected_proportions == [0.25, 0.25, 0.25, 0.25]

    def test_expected_proportions_normalization(self, srm_tester):
        """Test that expected proportions are normalized to sum to 1"""
        observed = [100, 100]
        expected = [2, 2]  # Should be normalized to [0.5, 0.5]
        result = srm_tester.test(observed, expected)

        assert result.expected_proportions == [0.5, 0.5]

    def test_numpy_array_input(self, srm_tester):
        """Test that numpy arrays work as input"""
        observed = np.array([500, 500])
        expected = np.array([0.5, 0.5])
        result = srm_tester.test(observed, expected)

        assert not result.reject_null
