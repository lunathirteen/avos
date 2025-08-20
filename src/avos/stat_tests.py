import numpy as np
from statsmodels.stats.proportion import proportions_ztest, test_proportions_2indep


def test_proportion_difference(
    count1, nobs1, count2, nobs2, method="ztest", alternative="two-sided", value=0
):
    """
    Test for difference in proportions between two independent samples.

    Parameters:
    -----------
    count1 : int
        Number of successes in first sample
    nobs1 : int
        Total number of observations in first sample
    count2 : int
        Number of successes in second sample
    nobs2 : int
        Total number of observations in second sample
    method : str, optional
        Method to use for testing:
        - 'ztest': Uses proportions_ztest (normal approximation)
        - 'indep': Uses test_proportions_2indep (more comprehensive test)
    alternative : str, optional
        The alternative hypothesis, one of:
        - 'two-sided': p1 != p2 (default)
        - 'larger': p1 > p2
        - 'smaller': p1 < p2
    value : float, optional
        The value of the null hypothesis (default is 0)

    Returns:
    --------
    dict
        A dictionary containing:
        - 'statistic': Test statistic
        - 'p_value': p-value for the test
        - 'proportion1': Observed proportion in first sample
        - 'proportion2': Observed proportion in second sample
        - 'difference': Difference in proportions (p2 - p1)
        - 'lift': Relative difference in proportions (p2 / p1 - 1)
    """
    # Calculate observed proportions
    prop1 = count1 / nobs1
    prop2 = count2 / nobs2
    diff = prop2 - prop1
    lift = prop2 / prop1 - 1

    if method == "ztest":
        # Use proportions_ztest
        counts = np.array([count1, count2])
        nobs = np.array([nobs1, nobs2])
        stat, pval = proportions_ztest(
            counts, nobs, alternative=alternative, value=value
        )

    elif method == "indep":
        # Use test_proportions_2indep (more comprehensive)
        result = test_proportions_2indep(
            count1, nobs1, count2, nobs2, alternative=alternative, value=value
        )
        stat = result.statistic
        pval = result.pvalue
    else:
        raise ValueError("Method must be either 'ztest' or 'indep'")

    return {
        "statistic": stat,
        "p_value": pval,
        "proportion1": prop1,
        "proportion2": prop2,
        "difference": diff,
        "lift": lift,
    }
