from scipy.stats import chisquare


def check_srm(observed_counts, expected_proportions=None, alpha=0.01):
    """
    Check for Sample Ratio Mismatch in A/B test data.

    Parameters:
    -----------
    observed_counts : list or tuple
        The actual counts of users/events in each variation.
    expected_proportions : list or tuple, optional
        The expected proportions for each variation. If None, equal proportions are assumed.
    alpha : float, optional
        The significance level for the SRM check. Default is 0.01.

    Returns:
    --------
    dict
        A dictionary containing:
        - 'srm_detected': Boolean indicating whether SRM was detected
        - 'observed_proportions': The actual proportions observed in the data
        - 'p_value': The p-value from the chi-square test
        - 'difference': The difference between max and min observed counts
        - 'lift': The ratio between max and min observed counts
    """
    # Validate inputs
    if not observed_counts or min(observed_counts) < 0:
        raise ValueError("Observed counts must be non-negative values")

    # Calculate total sample size
    total_count = sum(observed_counts)

    # If expected proportions not provided, assume equal split
    if expected_proportions is None:
        expected_proportions = [1 / len(observed_counts)] * len(observed_counts)

    # Validate expected proportions
    if len(expected_proportions) != len(observed_counts):
        raise ValueError("Length of expected_proportions must match observed_counts")
    if abs(sum(expected_proportions) - 1.0) > 1e-10:
        raise ValueError("Expected proportions must sum to 1")

    # Calculate expected counts
    expected_counts = [total_count * prop for prop in expected_proportions]

    # Perform chi-square test
    chi_result = chisquare(observed_counts, f_exp=expected_counts)
    p_value = chi_result[1]

    # Calculate difference
    observed_proportions = [count / total_count for count in observed_counts]
    max_difference = max(observed_counts) - min(observed_counts)
    max_lift = (max(observed_counts) - min(observed_counts)) / (
        total_count / len(observed_counts)
    )

    # Determine if SRM is detected
    srm_detected = p_value < alpha

    return {
        "srm_detected": srm_detected,
        "observed_proportions": observed_proportions,
        "p_value": p_value,
        "difference": max_difference,
        "lift": max_lift,
    }
