import math

import matplotlib.pyplot as plt
import numpy as np
from scipy.optimize import brentq
from statsmodels.stats.power import NormalIndPower, TTestIndPower

# ===========================
# SAMPLE SIZE CALCULATION
# ===========================

def calculate_sample_size_proportions(
    baseline_rate: float,
    mde: float,
    alpha: float = 0.05,
    power: float = 0.8,
    alternative: str = 'two-sided'
) -> int:
    """
    Calculate the required sample size per variant for a two-sample proportion test.

    Parameters:
    -----------
    baseline_rate : float
        Baseline conversion rate (p0), must be between 0 and 1.
    mde : float
        Minimum detectable effect as a proportion of the baseline (e.g., 0.1 for a 10% increase).
        Must be non-negative, and baseline_rate*(1+mde) should be <= 1.
    alpha : float
        Significance level, default is 0.05.
    power : float
        Statistical power, default is 0.8.
    alternative : str
        - 'two-sided'
        - 'larger'
        - 'smaller'

    Returns:
    --------
    dict
        'sample_size': Required sample size per group.
    """

    # Validate inputs
    if not 0 < baseline_rate < 1:
        raise ValueError("baseline_rate must be between 0 and 1.")
    if mde <= 0:
        raise ValueError("mde must be positive (representing a relative change).")

    target_rate = baseline_rate * (1 + mde)
    if target_rate >= 1:
        raise ValueError("The calculated target rate must be less than 1. Please adjust baseline_rate or mde.")

    # Calculate effect size using Cohen's h
    # Cohen's h = 2 * arcsin(sqrt(p1)) - 2 * arcsin(sqrt(p0))
    # https://stats.stackexchange.com/a/653103
    def cohen_h(p1: float, p0: float) -> float:
        return 2 * math.asin(math.sqrt(p1)) - 2 * math.asin(math.sqrt(p0))

    effect_size = abs(cohen_h(target_rate, baseline_rate))

    # Create a power analysis object for a two-sample z-test for proportions
    analysis = NormalIndPower()
    sample_size = analysis.solve_power(effect_size=effect_size, alpha=alpha, power=power, alternative=alternative)

    return { 'sample_size': math.ceil(sample_size) }


def calculate_sample_size_continuous(
    baseline_mean: float,
    mde: float,
    std: float,
    alpha: float = 0.05,
    power: float = 0.8,
    alternative: str = 'two-sided'
) -> int:
    """
    Calculate the required sample size per group for a two-sample t-test
    for continuous outcomes using baseline mean and a relative minimum detectable effect.

    Parameters:
    -----------
    baseline_mean : float
        The baseline mean of the outcome.
    mde : float
        The minimum detectable effect as a relative change (e.g., 0.1 for a 10% change). Must be positive.
    std : float
        The standard deviation of the outcome.
    alpha : float
        Significance level (default is 0.05).
    power : float
        Statistical power (default is 0.8).
    alternative : str
        - 'two-sided'
        - 'larger'
        - 'smaller'

    Returns:
    --------
    dict
        'sample_size': Required sample size per group.
    """
    # Validate inputs
    if std <= 0:
        raise ValueError("std must be positive.")
    if mde <= 0:
        raise ValueError("mde must be positive (representing a relative change).")

    # Calculate the absolute difference (delta) based on baseline mean and mde.
    delta = baseline_mean * mde

    # Compute standardized effect size (Cohen's d)
    effect_size = delta / std

    # Create a power analysis object for a two-sample t-test
    analysis = TTestIndPower()
    sample_size = analysis.solve_power(effect_size=effect_size, alpha=alpha, power=power, alternative=alternative)

    # Return the ceiling of the computed sample size (per group)
    return { 'sample_size' : math.ceil(sample_size) }


# ===========================
# POWER CURVES
# ===========================

def plot_power_curve_continuous(
    baseline_mean: float,
    std: float,
    alpha: float = 0.05,
    power: float = 0.8,
    mde_min: float = 0.01,
    mde_max: float = 0.5,
    num_points: int = 50
) -> None:
    """
    Plot the required sample size (per group) as a function of relative MDE
    for a continuous metric. The MDE is expressed as a relative change
    with respect to the baseline_mean.
    """
    mde_values = np.linspace(mde_min, mde_max, num_points)
    sample_sizes = []

    for mde in mde_values:
        # Calculate Sample Size
        n = calculate_sample_size_continuous(baseline_mean=baseline_mean, mde=mde, std=std)
        sample_sizes.append(n)

    plt.figure(figsize=(8, 5))
    plt.plot(mde_values, sample_sizes, marker='o')
    plt.xlabel("Minimum Detectable Effect (relative change)")
    plt.ylabel("Required Sample Size per Group")
    plt.title("Power Curve for Continuous Metric")
    plt.grid(True)
    plt.show()


def plot_power_curve_proportions(
    baseline_rate: float,
    alpha: float = 0.05,
    power: float = 0.8,
    mde_min: float = 0.01,
    mde_max: float = 0.5,
    num_points: int = 50
) -> None:
    """
    Plot the required sample size (per group) as a function of relative MDE
    for a proportion metric. The MDE is expressed as a relative change with
    respect to the baseline_rate.
    """
    mde_values = np.linspace(mde_min, mde_max, num_points)
    sample_sizes = []

    for mde in mde_values:
        # Calculate Sample Size
        n = calculate_sample_size_proportions(baseline_rate=baseline_rate, mde=mde)
        sample_sizes.append(n)

    plt.figure(figsize=(8, 5))
    plt.plot(mde_values, sample_sizes, marker='o')
    plt.xlabel("Minimum Detectable Effect (relative change)")
    plt.ylabel("Required Sample Size per Group")
    plt.title("Power Curve for Proportions")
    plt.grid(True)
    plt.show()


# ===========================
# SENSITIVITY ANALYSIS
# ===========================

def sensitivity_analysis_continuous(
    baseline_mean: float,
    std: float,
    sample_size: int,
    alpha: float = 0.05,
    power: float = 0.8,
    alternative: str = 'two-sided'
) -> float:
    """
    For a continuous outcome, compute the minimum detectable effect (relative to baseline_mean)
    given a fixed sample size per group, significance level, and desired power.

    Parameters:
    -----------
    baseline_mean : float
        The baseline mean of the outcome.
    std : float
        The standard deviation of the outcome.
    sample_size : int
        The number of observations
    alpha : float
        Significance level (default is 0.05).
    power : float
        Statistical power (default is 0.8).
    alternative : str
        - 'two-sided'
        - 'larger'
        - 'smaller'

    Returns:
    --------
    dict
        'mde': The relative minimum detectable effect (e.g., 0.1 means a 10% change).
    """
    analysis = TTestIndPower()
    # Solve for the standardized effect size (Cohen's d) with a fixed sample size:
    effect_size = analysis.solve_power(effect_size=None, nobs1=sample_size,
                                       alpha=alpha, power=power, alternative=alternative)
    # Convert Cohen's d back to an absolute difference
    delta = effect_size * std
    # Express as a relative change (MDE)
    mde = delta / baseline_mean
    return { 'mde' : mde }


def sensitivity_analysis_proportions(
    baseline_rate: float,
    sample_size: int,
    alpha: float = 0.05,
    power: float = 0.8,
    alternative: str = 'two-sided'
) -> float:
    """
    For a proportion outcome, compute the minimum detectable effect (relative change)
    given a fixed sample size per group, significance level, and desired power.

    Parameters:
    -----------
    baseline_rate : float
        Baseline conversion rate (p0), must be between 0 and 1.
    sample_size : int
        The number of observations
    alpha : float
        Significance level, default is 0.05.
    power : float
        Statistical power, default is 0.8.
    alternative : str
        - 'two-sided'
        - 'larger'
        - 'smaller'

    Returns:
    --------
    dict
        'mde': The relative minimum detectable effect (e.g., 0.1 means a 10% change).

    This involves solving for mde numerically since the effect size is computed via Cohen's h:
        h = 2 * arcsin(sqrt(baseline_rate*(1+mde))) - 2 * arcsin(sqrt(baseline_rate))
    """
    analysis = NormalIndPower()
    # First, find the required standardized effect size given the fixed sample size:
    effect_size = analysis.solve_power(effect_size=None, nobs1=sample_size,
                                       alpha=alpha, power=power, alternative=alternative)

    # Define a function that calculates the difference between the computed h and the target effect_size
    def h_diff(mde):
        target_rate = baseline_rate * (1 + mde)
        if target_rate >= 1:
            return effect_size  # beyond valid range, just return positive diff
        h = abs(2 * math.asin(math.sqrt(target_rate)) - 2 * math.asin(math.sqrt(baseline_rate)))
        return h - effect_size

    # Solve for mde. We know mde must be > 0 and less than (1 - baseline_rate)/baseline_rate.
    mde_lower = 0.0001
    mde_upper = (1 - baseline_rate) / baseline_rate - 0.0001
    try:
        mde = brentq(h_diff, mde_lower, mde_upper)
    except Exception:
        mde = float('nan')
    return { 'mde' : mde }


if __name__ == '__main__':
    BASELINE_RATE = 0.278533 #0.106365
    MDE = 0.5

    sample_size = calculate_sample_size_proportions(BASELINE_RATE, MDE)
    print(sample_size)

    plot_power_curve_proportions(BASELINE_RATE, mde_min = 0.1, mde_max=0.5)
