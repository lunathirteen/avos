from datetime import timedelta
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from avos.models.base import Base
from avos.models.experiment import Experiment, ExperimentStatus
from avos.services.layer_service import LayerService
from avos.services.splitter import AssignmentService
from avos.utils.datetime_utils import utc_now, to_utc


def main():
    # Database setup
    engine = create_engine("sqlite:///ecommerce_experiments.db", echo=False)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    # =============================================================================
    # SCENARIO: E-commerce site with multiple experiment layers
    # =============================================================================

    print("🚀 Setting up E-commerce A/B Testing Platform\n")

    # Layer 1: Homepage Hero Section (High Traffic)
    print("📍 Creating Homepage Layer...")
    homepage_layer = LayerService.create_layer(
        session,
        layer_id="homepage_hero",
        layer_salt="homepage_salt_2025",
        total_slots=100,  # High capacity for main page
        total_traffic_percentage=100.0,
    )

    # Layer 2: Checkout Flow (Lower Traffic, High Value)
    print("📍 Creating Checkout Layer...")
    checkout_layer = LayerService.create_layer(
        session,
        layer_id="checkout_flow",
        layer_salt="checkout_salt_2025",
        total_slots=100,  # Lower traffic but critical funnel
        total_traffic_percentage=100.0,
    )

    # Layer 3: Product Recommendation Engine
    print("📍 Creating Recommendations Layer...")
    recommendations_layer = LayerService.create_layer(
        session,
        layer_id="product_recommendations",
        layer_salt="recs_salt_2025",
        total_slots=100,
        total_traffic_percentage=80.0,  # Only 80% get experimental recommendations
    )

    # =============================================================================
    # EXPERIMENT 1: Homepage Hero Button Colors (Classic A/B Test)
    # =============================================================================

    print("\n🧪 Adding Homepage Button Color Experiment...")
    hero_button_exp = Experiment(
        experiment_id="hero_button_colors_v2",
        layer_id="homepage_hero",
        name="Hero CTA Button Color Test v2",
        variants=["blue_primary", "green_convert", "red_urgent"],
        traffic_allocation={"blue_primary": 40, "green_convert": 35, "red_urgent": 25},
        traffic_percentage=60.0,  # 60% of homepage visitors
        start_date=utc_now() - timedelta(hours=2),
        end_date=utc_now() + timedelta(days=14),
        status=ExperimentStatus.ACTIVE,
        priority=1,
    )

    success = LayerService.add_experiment(session, homepage_layer, hero_button_exp)
    print(f"✅ Hero button experiment added: {success}")

    # =============================================================================
    # EXPERIMENT 2: Homepage Layout (Multivariate)
    # =============================================================================

    print("\n🧪 Adding Homepage Layout Experiment...")
    layout_exp = Experiment(
        experiment_id="homepage_layout_2025",
        layer_id="homepage_hero",
        name="Homepage Layout Optimization",
        variants=["layout_minimal", "layout_feature_rich", "layout_testimonials"],
        traffic_allocation={"layout_minimal": 33, "layout_feature_rich": 34, "layout_testimonials": 33},
        traffic_percentage=25.0,  # 25% additional traffic (total: 85% homepage experimentation)
        start_date=utc_now() - timedelta(days=1),
        end_date=utc_now() + timedelta(days=21),
        status=ExperimentStatus.ACTIVE,
        priority=2,
    )

    success = LayerService.add_experiment(session, homepage_layer, layout_exp)
    print(f"✅ Layout experiment added: {success}")

    # =============================================================================
    # EXPERIMENT 3: Checkout Payment Methods
    # =============================================================================

    print("\n🧪 Adding Checkout Payment Experiment...")
    payment_exp = Experiment(
        experiment_id="checkout_payment_methods",
        layer_id="checkout_flow",
        name="Payment Methods Order Test",
        variants=["credit_card_first", "paypal_prominent", "buy_now_pay_later_top"],
        traffic_allocation={"credit_card_first": 50, "paypal_prominent": 30, "buy_now_pay_later_top": 20},
        traffic_percentage=75.0,  # 75% of checkout users
        start_date=utc_now() - timedelta(hours=6),
        end_date=utc_now() + timedelta(days=10),
        status=ExperimentStatus.ACTIVE,
        priority=1,
    )

    success = LayerService.add_experiment(session, checkout_layer, payment_exp)
    print(f"✅ Payment methods experiment added: {success}")

    # =============================================================================
    # EXPERIMENT 4: ML Recommendation Algorithm
    # =============================================================================

    print("\n🧪 Adding ML Recommendations Experiment...")
    ml_recs_exp = Experiment(
        experiment_id="ml_recommendations_v3",
        layer_id="product_recommendations",
        name="ML Recommendation Algorithm v3.0",
        variants=["collaborative_filtering", "deep_learning_v2", "hybrid_ensemble", "trending_boost"],
        traffic_allocation={
            "collaborative_filtering": 25,
            "deep_learning_v2": 35,
            "hybrid_ensemble": 25,
            "trending_boost": 15,
        },
        traffic_percentage=95.0,  # Almost all recommendation traffic (95% of 80% layer capacity)
        start_date=utc_now() - timedelta(days=3),
        end_date=utc_now() + timedelta(days=30),
        status=ExperimentStatus.ACTIVE,
        priority=1,
    )

    success = LayerService.add_experiment(session, recommendations_layer, ml_recs_exp)
    print(f"✅ ML recommendations experiment added: {success}")

    # =============================================================================
    # EXPERIMENT 5: Seasonal Promotion (Time-bound)
    # =============================================================================

    print("\n🧪 Adding Seasonal Promotion Experiment...")
    seasonal_exp = Experiment(
        experiment_id="black_friday_banner_2025",
        layer_id="homepage_hero",
        name="Black Friday Banner Messaging",
        variants=["discount_percent", "limited_time", "countdown_timer", "social_proof"],
        traffic_allocation={"discount_percent": 30, "limited_time": 25, "countdown_timer": 25, "social_proof": 20},
        traffic_percentage=10.0,  # Small remaining capacity (total now: 95%)
        start_date=utc_now() + timedelta(days=2),  # Starts in future
        end_date=utc_now() + timedelta(days=5),  # Short campaign
        status=ExperimentStatus.ACTIVE,
        priority=3,
    )

    success = LayerService.add_experiment(session, homepage_layer, seasonal_exp)
    print(f"✅ Seasonal experiment added: {success}")

    # =============================================================================
    # LAYER STATISTICS & CAPACITY ANALYSIS
    # =============================================================================

    print("\n📊 LAYER UTILIZATION ANALYSIS")
    print("=" * 50)

    for layer_name, layer in [
        ("Homepage", homepage_layer),
        ("Checkout", checkout_layer),
        ("Recommendations", recommendations_layer),
    ]:
        info = LayerService.get_layer_info(session, layer)
        print(f"\n🎯 {layer_name} Layer ({layer.layer_id}):")
        print(f"   Total Slots: {info['total_slots']:,}")
        print(f"   Used Slots: {info['used_slots']:,}")
        print(f"   Free Slots: {info['free_slots']:,}")
        print(f"   Utilization: {info['utilization_percentage']:.1f}%")
        print(f"   Active Experiments: {info['active_experiments']}")
        print("   Slot Distribution:")
        for exp_id, slot_count in info["experiment_slot_counts"].items():
            print(f"      {exp_id}: {slot_count:,} slots")

    # =============================================================================
    # USER ASSIGNMENT SIMULATION
    # =============================================================================

    print("\n👥 USER ASSIGNMENT SIMULATION")
    print("=" * 50)

    # Simulate different user types
    user_scenarios = {
        "homepage_visitors": list(range(1000, 1200)),  # 200 homepage visitors
        "checkout_users": list(range(5000, 5050)),  # 50 checkout users
        "product_browsers": list(range(8000, 8100)),  # 100 product page visitors
        "mobile_users": [f"mobile_{i}" for i in range(75)],  # 75 mobile users
        "returning_customers": [f"customer_{i}" for i in range(2000, 2025)],  # 25 returning customers
    }

    for scenario_name, user_ids in user_scenarios.items():
        print(f"\n🔍 {scenario_name.replace('_', ' ').title()} Assignments:")

        # Homepage assignments
        AssignmentService.get_user_assignments_bulk(session, homepage_layer, user_ids[: min(50, len(user_ids))])
        homepage_summary = AssignmentService.preview_assignment_distribution(session, homepage_layer, user_ids)

        print(f"   Homepage Layer - Assignment Rate: {homepage_summary['assignment_rate']:.1f}%")
        for variant_key, count in homepage_summary["assignment_distribution"].items():
            print(f"      {variant_key}: {count} users")

        # Checkout assignments (for relevant users)
        if scenario_name in ["checkout_users", "returning_customers"]:
            checkout_summary = AssignmentService.preview_assignment_distribution(session, checkout_layer, user_ids)
            print(f"   Checkout Layer - Assignment Rate: {checkout_summary['assignment_rate']:.1f}%")
            for variant_key, count in checkout_summary["assignment_distribution"].items():
                print(f"      {variant_key}: {count} users")

    # =============================================================================
    # INDIVIDUAL USER JOURNEY TRACKING
    # =============================================================================

    print("\n🎬 INDIVIDUAL USER JOURNEY EXAMPLES")
    print("=" * 50)

    sample_users = ["user_12345", "user_67890", "customer_premium_001"]

    for user_id in sample_users:
        print(f"\n👤 User Journey: {user_id}")

        # Homepage experience
        homepage_assignment = AssignmentService.get_user_assignment(session, homepage_layer, user_id)
        print(f"   📱 Homepage: {homepage_assignment['status']}")
        if homepage_assignment["status"] == "assigned":
            print(f"      Experiment: {homepage_assignment['experiment_name']}")
            print(f"      Variant: {homepage_assignment['variant']}")
            print(f"      Slot: {homepage_assignment['slot_index']}")

        # Checkout experience (if user converts)
        checkout_assignment = AssignmentService.get_user_assignment(session, checkout_layer, user_id)
        print(f"   💳 Checkout: {checkout_assignment['status']}")
        if checkout_assignment["status"] == "assigned":
            print(f"      Experiment: {checkout_assignment['experiment_name']}")
            print(f"      Variant: {checkout_assignment['variant']}")

        # Recommendations experience
        recs_assignment = AssignmentService.get_user_assignment(session, recommendations_layer, user_id)
        print(f"   🤖 Recommendations: {recs_assignment['status']}")
        if recs_assignment["status"] == "assigned":
            print(f"      Algorithm: {recs_assignment['variant']}")

    # =============================================================================
    # EXPERIMENT LIFECYCLE MANAGEMENT
    # =============================================================================

    print("\n⏰ EXPERIMENT LIFECYCLE MANAGEMENT")
    print("=" * 50)

    # Check which experiments are currently active
    print("\nActive Experiments Right Now:")
    all_experiments = session.execute(select(Experiment)).scalars().all()
    for exp in all_experiments:
        active_status = "✅ ACTIVE" if exp.is_active() else "❌ INACTIVE"
        print(f"   {exp.experiment_id}: {active_status}")
        if not exp.is_active():
            if exp.start_date and utc_now() < to_utc(exp.start_date):
                print(f"      → Starts in {to_utc(exp.start_date) - utc_now()}")
            elif exp.end_date and utc_now() > to_utc(exp.end_date):
                print(f"      → Ended {utc_now() - to_utc(exp.end_date)} ago")
            elif exp.status != ExperimentStatus.ACTIVE:
                print(f"      → Status: {exp.status.value}")

    # Simulate removing a completed experiment
    print("\n🔄 Removing completed experiment...")
    removed = LayerService.remove_experiment(session, homepage_layer, "hero_button_colors_v2")
    print(f"   Experiment removed: {removed}")

    if removed:
        updated_info = LayerService.get_layer_info(session, homepage_layer)
        print(f"   Homepage layer utilization now: {updated_info['utilization_percentage']:.1f}%")

    # =============================================================================
    # PERFORMANCE & DISTRIBUTION ANALYSIS
    # =============================================================================

    print("\n📈 STATISTICAL ANALYSIS")
    print("=" * 50)

    # Large-scale distribution test
    large_sample = list(range(10000))  # 10K users

    print("Distribution Analysis (10,000 user sample):")
    for layer_name, layer in [("Homepage", homepage_layer), ("Recommendations", recommendations_layer)]:
        distribution = AssignmentService.preview_assignment_distribution(session, layer, large_sample)

        print(f"\n📊 {layer_name} Layer Distribution:")
        print(f"   Assignment Rate: {distribution['assignment_rate']:.2f}%")
        print(f"   Assigned Users: {len(large_sample) - distribution['unassigned_count']:,}")
        print(f"   Unassigned Users: {distribution['unassigned_count']:,}")

        print("   Variant Distribution:")
        total_assigned = sum(distribution["assignment_distribution"].values())
        for variant_key, count in sorted(distribution["assignment_distribution"].items()):
            percentage = (count / total_assigned * 100) if total_assigned > 0 else 0
            print(f"      {variant_key}: {count:,} users ({percentage:.1f}%)")

    # =============================================================================
    # ADVANCED SCENARIOS
    # =============================================================================

    print("\n🎯 ADVANCED USAGE SCENARIOS")
    print("=" * 50)

    # Scenario 1: Experiment Conflicts (trying to exceed capacity)
    print("\n💥 Testing Capacity Limits...")
    overload_exp = Experiment(
        experiment_id="overload_test",
        layer_id="homepage_hero",
        name="This Should Fail - Over Capacity",
        variants=["variant_a"],
        traffic_allocation={"variant_a": 100},
        traffic_percentage=110.0,  # This would exceed remaining capacity
        status=ExperimentStatus.ACTIVE,
    )

    success = LayerService.add_experiment(session, homepage_layer, overload_exp)
    print(f"   Over-capacity experiment success: {success} (should be False)")

    # Scenario 2: Consistent user experience across sessions
    print("\n🔒 Testing Assignment Consistency...")
    test_user = "consistency_test_user"
    assignments = []
    for i in range(5):
        assignment = AssignmentService.get_user_assignment(session, homepage_layer, test_user)
        assignments.append(assignment["variant"] if assignment["status"] == "assigned" else None)

    all_same = len(set(assignments)) <= 1
    print(f"   User gets consistent assignment: {all_same} (variants: {set(assignments)})")

    # Scenario 3: Different salt produces different distributions
    print("\n🧂 Testing Salt Impact on Distribution...")
    salt_layer_a = LayerService.create_layer(session, "salt_test_a", "salt_a", total_slots=100)
    salt_layer_b = LayerService.create_layer(session, "salt_test_b", "salt_b", total_slots=100)

    sample_users = [f"user_{i}" for i in range(100)]
    slots_a = [AssignmentService._calculate_user_slot(salt_layer_a.layer_salt, 100, user) for user in sample_users]
    slots_b = [AssignmentService._calculate_user_slot(salt_layer_b.layer_salt, 100, user) for user in sample_users]

    different_distributions = len(set(slots_a) & set(slots_b)) < 90  # Should have significant differences
    print(f"   Different salts create different distributions: {different_distributions}")

    print("\n🎉 Complex A/B Testing Platform Demo Complete!")
    print("=" * 50)

    session.close()


if __name__ == "__main__":
    main()
