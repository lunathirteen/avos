from avos.srm_tester import SRMTester


def main():
    # Initialize SRM tester
    srm_tester = SRMTester(alpha=0.05)

    print("\nCheck a balanced experiment")
    observed_counts = [4950, 5050]  # Control: 4950, Treatment: 5050
    expected_proportions = [0.5, 0.5]  # 50/50 split

    result = srm_tester.test(observed_counts, expected_proportions)
    print(result)
    # Output: No SRM (χ²=1.000, p=0.317311)

    if result.reject_null:
        print("⚠️  SRM detected! Check your randomization.")
    else:
        print("✅ No SRM - safe to proceed with analysis")

    print("\nThree-arm test: Control, Treatment A, Treatment B")
    observed = [2000, 1800, 2200]  # Uneven distribution
    expected = [0.33, 0.33, 0.34]  # Target allocation

    result = srm_tester.test(observed, expected)
    print(f"SRM Status: {result}")
    print(f"Sample size: {result.total_sample_size:,}")
    print(f"Degrees of freedom: {result.degrees_of_freedom}")

    # Check allocation deviation
    for i, (obs, exp) in enumerate(zip(result.observed_counts, result.expected_proportions)):
        actual_prop = obs / result.total_sample_size
        deviation = ((actual_prop - exp) / exp) * 100
        print(f"Group {i+1}: {obs:,} ({actual_prop:.1%}) vs {exp:.1%} expected ({deviation:+.1f}%)")

    print("\nTest multiple experiments at once")
    experiments_data = {
        "checkout_flow_v2": {"observed": [4850, 5150], "expected": [0.5, 0.5]},
        "button_color_test": {"observed": [1200, 800], "expected": [0.5, 0.5]},  # Clear imbalance
        "pricing_experiment": {"observed": [2500, 2000, 1500], "expected": [0.4, 0.35, 0.25]},  # Three groups
    }

    results = srm_tester.batch_test(experiments_data)

    print("SRM Health Check Report")
    print("=" * 50)

    for exp_name, result in results.items():
        status_icon = "🔴" if result.reject_null else "🟢"
        print(f"{status_icon} {exp_name} p={result.p_value:.6f} {result.severity}")
        print(f"   Sample size: {result.total_sample_size:,}")

        if result.reject_null:
            print("   ⚠️  Action required: Check randomization logic")
        print()

    print(srm_tester.significance_legend())


if __name__ == "__main__":
    main()
