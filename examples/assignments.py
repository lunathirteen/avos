from datetime import datetime, timedelta
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Import your AVOS components
from avos.models.base import Base
from avos.models.experiment import Experiment, ExperimentStatus
from avos.services.layer_service import LayerService
from avos.services.splitter import AssignmentService
from avos.utils.datetime_utils import utc_now

def main():
    # Database setup
    engine = create_engine("sqlite:///avos_demo.db", echo=False)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    print("🚀 AVOS Assignment System Demo\n")

    # Step 1: Create layers for different parts of your application
    print("📍 Creating layers...")

    homepage_layer = LayerService.create_layer(
        session,
        layer_id="homepage_hero",
        layer_salt="homepage_salt_2025",
        total_slots=1000,
        total_traffic_percentage=100.0,
    )

    checkout_layer = LayerService.create_layer(
        session,
        layer_id="checkout_flow",
        layer_salt="checkout_salt_2025",
        total_slots=500,
        total_traffic_percentage=100.0,
    )

    # Step 2: Create and add experiments to layers
    print("🧪 Adding experiments...")

    # Homepage button experiment
    button_exp = Experiment(
        experiment_id="hero_button_colors",
        layer_id="homepage_hero",
        name="Hero Button Color Test",
        variants=["blue", "green", "red"],
        traffic_allocation={"blue": 33.33, "green": 33.33, "red": 33.34},
        traffic_percentage=60.0,
        start_date=utc_now() - timedelta(hours=1),
        end_date=utc_now() + timedelta(days=14),
        status=ExperimentStatus.ACTIVE,
        priority=1,
    )

    success = LayerService.add_experiment(session, homepage_layer, button_exp)
    print(f"✅ Button experiment added: {success}")

    # Checkout payment experiment
    payment_exp = Experiment(
        experiment_id="checkout_payment",
        layer_id="checkout_flow",
        name="Payment Methods Test",
        variants=["credit_first", "paypal_first"],
        traffic_allocation={"credit_first": 50.0, "paypal_first": 50.0},
        traffic_percentage=75.0,
        start_date=utc_now() - timedelta(hours=2),
        end_date=utc_now() + timedelta(days=10),
        status=ExperimentStatus.ACTIVE,
        priority=1,
    )

    success = LayerService.add_experiment(session, checkout_layer, payment_exp)
    print(f"✅ Payment experiment added: {success}")

    # Step 3: Single user assignment
    print("\n👤 Single user assignment demo...")

    user_assignment = AssignmentService.get_user_assignment(
        session, homepage_layer, "user_12345"
    )
    print(f"User assignment: {user_assignment}")

    # Step 4: Bulk user assignments
    print("\n👥 Bulk user assignment demo...")

    user_ids = [f"user_{i:06d}" for i in range(1000)]
    bulk_assignments = AssignmentService.get_user_assignments_bulk(
        session, homepage_layer, user_ids
    )

    print(f"Processed {len(bulk_assignments)} user assignments")

    # Step 5: Assignment distribution preview
    print("\n📊 Assignment distribution preview...")

    preview = AssignmentService.preview_assignment_distribution(
        session, homepage_layer, user_ids[:100]
    )
    print(f"Distribution preview: {preview}")

    # Step 6: Query DuckDB assignment logs
    print("\n🗄️ Querying assignment logs from DuckDB...")

    logger = AssignmentService._assignment_logger

    # Query variant counts for an experiment
    variant_counts = logger.con.execute("""
        SELECT experiment_id, variant, COUNT(*) as count
        FROM user_assignments
        WHERE experiment_id = 'hero_button_colors'
          AND status = 'assigned'
        GROUP BY experiment_id, variant
        ORDER BY count DESC
    """).fetchall()

    print("Variant distribution:")
    for row in variant_counts:
        print(f"  {row[0]} - {row[1]}: {row[2]} users")

    # Query assignment status breakdown
    status_breakdown = logger.con.execute("""
        SELECT status, COUNT(*) as count
        FROM user_assignments
        GROUP BY status
    """).fetchall()

    print("\nAssignment status breakdown:")
    for row in status_breakdown:
        print(f"  {row[0]}: {row[1]} assignments")

    # Query recent assignments
    recent_assignments = logger.con.execute("""
        SELECT unit_id, experiment_id, variant, assignment_timestamp
        FROM user_assignments
        WHERE status = 'assigned'
        ORDER BY assignment_timestamp DESC
        LIMIT 5
    """).fetchall()

    print("\nRecent assignments:")
    for row in recent_assignments:
        print(f"  {row[0]} -> {row[1]}:{row[2]} at {row[3]}")

    # Step 7: Layer utilization analysis
    print("\n📈 Layer utilization analysis...")

    for layer in [homepage_layer, checkout_layer]:
        info = LayerService.get_layer_info(session, layer)
        print(f"\n🎯 Layer: {layer.layer_id}")
        print(f"   Utilization: {info['utilization_percentage']:.1f}%")
        print(f"   Used slots: {info['used_slots']:,}/{info['total_slots']:,}")
        print(f"   Active experiments: {info['active_experiments']}")

    # Step 8: Cleanup
    print("\n🧹 Cleaning up...")
    AssignmentService._assignment_logger.close()
    session.close()

    print("Demo completed successfully! 🎉")

if __name__ == "__main__":
    main()
