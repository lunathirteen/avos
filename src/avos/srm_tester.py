from scipy.stats import chisquare, chi2
import numpy as np
from typing import Dict, List, Optional, Union, Tuple
from dataclasses import dataclass
from datetime import datetime
import json


@dataclass
class SRMResult:
    """Structured SRM test result"""

    chi2_stat: float
    p_value: float
    degrees_of_freedom: int
    severity: str  # R-style signif. codes:  0 ‘***’ 0.001 ‘**’ 0.01 ‘*’ 0.05 ‘.’ 0.1 ‘ ’ 1
    reject_null: bool
    observed_counts: List[int]
    expected_counts: List[float]
    expected_proportions: List[float]
    total_sample_size: int

    def __str__(self):
        status = "SRM DETECTED" if self.reject_null else "No SRM"
        significance = f" {self.severity}" if self.severity else ""
        return f"{status} (χ²={self.chi2_stat:.3f}, p={self.p_value:.6f}{significance})"


class SRMTester:
    """Sample Ratio Mismatch detector"""

    def __init__(self, alpha: float = 0.05):
        self.alpha = alpha

    def test(
        self,
        observed_counts: Union[List[int], np.ndarray],
        expected_proportions: Optional[Union[List[float], np.ndarray]] = None,
        experiment_id: Optional[str] = None,
    ) -> SRMResult:
        """
        Perform SRM test on assignment counts

        Args:
            observed_counts: Actual assignment counts per group [n_control, n_treatment_1, ...]
            expected_proportions: Expected allocation ratios [0.5, 0.5] or [0.33, 0.33, 0.34]
            experiment_id: Optional experiment identifier for logging

        Returns:
            SRMResult object
        """
        observed_counts = np.array(observed_counts, dtype=int)

        # Validate input
        if len(observed_counts) < 2:
            raise ValueError("Need at least 2 groups for SRM test")
        if np.any(observed_counts < 0):
            raise ValueError("Observed counts must be non-negative")

        # Set default equal proportions if not provided
        if expected_proportions is None:
            expected_proportions = np.ones(len(observed_counts)) / len(observed_counts)
        else:
            expected_proportions = np.array(expected_proportions, dtype=float)
            # Normalize to ensure they sum to 1
            expected_proportions = expected_proportions / expected_proportions.sum()

        if len(expected_proportions) != len(observed_counts):
            raise ValueError("Expected proportions must match number of groups")

        # Calculate expected counts
        total_sample_size = observed_counts.sum()
        expected_counts = expected_proportions * total_sample_size

        # Perform chi-square test
        chi2_stat, p_value = chisquare(f_obs=observed_counts, f_exp=expected_counts)
        degrees_of_freedom = len(observed_counts) - 1

        # Significance classification
        severity = self._classify_severity(p_value)
        reject_null = p_value < self.alpha

        return SRMResult(
            chi2_stat=chi2_stat,
            p_value=p_value,
            degrees_of_freedom=degrees_of_freedom,
            severity=severity,
            reject_null=reject_null,
            observed_counts=observed_counts.tolist(),
            expected_counts=expected_counts.tolist(),
            expected_proportions=expected_proportions.tolist(),
            total_sample_size=total_sample_size,
        )

    def _classify_severity(self, p_value: float) -> str:
        """
        Classify significance using R-style codes:
        *** : p ≤ 0.001
        **  : 0.001 < p ≤ 0.01
        *   : 0.01 < p ≤ 0.05
        .   : 0.05 < p ≤ 0.1
        ""  : p > 0.1
        """
        if p_value <= 0.001:
            return "***"
        elif p_value <= 0.01:
            return "**"
        elif p_value <= 0.05:
            return "*"
        elif p_value <= 0.1:
            return "."
        else:
            return ""

    def batch_test(self, experiments_data: Dict[str, Dict]) -> Dict[str, SRMResult]:
        """Test multiple experiments for SRM in batch"""
        results = {}
        for exp_id, data in experiments_data.items():
            try:
                results[exp_id] = self.test(
                    observed_counts=data["observed"], expected_proportions=data.get("expected"), experiment_id=exp_id
                )
            except Exception as e:
                print(f"SRM test failed for {exp_id}: {e}")

        return results

    def critical_value(self, degrees_of_freedom: int, alpha: Optional[float] = None) -> float:
        """Get critical chi-square value for given degrees of freedom"""
        alpha = alpha or self.alpha
        return chi2.ppf(1 - alpha, degrees_of_freedom)

    def significance_legend(self) -> str:
        """Return R-style significance legend"""
        return """---\nSignif. codes:  0 '***' 0.001 '**' 0.01 '*' 0.05 '.' 0.1 '' 1"""
