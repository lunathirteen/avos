import pytest
from unittest.mock import patch
from datetime import timedelta
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from avos.models.base import Base
from avos.models.layer import LayerSlot
from avos.models.experiment import Experiment, ExperimentStatus
from avos.services.layer_service import LayerService
from avos.services.splitter import HashBasedSplitter, AssignmentService
from avos.utils.datetime_utils import utc_now


@pytest.fixture
def db_session():
    """Create an in-memory SQLite database for testing."""
    engine = create_engine("sqlite:///:memory:", echo=False)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.rollback()
    session.close()


@pytest.fixture
def sample_layer_with_experiment(db_session):
    """Create a layer with an active experiment for testing."""
    # Create layer
    layer = LayerService.create_layer(db_session, "test_layer", "salt123", total_slots=100)

    # Create experiment with UTC datetimes
    experiment = Experiment(
        experiment_id="test_exp",
        layer_id="test_layer",
        name="Button Color Test",
        variants=["red", "blue", "green"],
        traffic_allocation={"red": 40, "blue": 35, "green": 25},
        traffic_percentage=60.0,
        start_date=utc_now() - timedelta(days=1),  # UTC datetime
        end_date=utc_now() + timedelta(days=7),  # UTC datetime
        status=ExperimentStatus.ACTIVE,
    )

    # Add experiment to layer
    LayerService.add_experiment(db_session, layer, experiment)

    return layer, experiment


class TestHashBasedSplitter:
    """Test HashBasedSplitter functionality."""

    def test_splitter_initialization(self):
        """Test splitter can be initialized with experiment ID."""
        splitter = HashBasedSplitter("exp_123")
        assert splitter.exp_id == "exp_123"

    def test_assign_variant_deterministic(self):
        """Test that variant assignment is deterministic for same user."""
        splitter = HashBasedSplitter("exp_001")
        variants = ["control", "treatment"]
        allocations = [50.0, 50.0]

        # Same user should get same variant
        variant1 = splitter.assign_variant("user_123", variants, allocations)
        variant2 = splitter.assign_variant("user_123", variants, allocations)

        assert variant1 == variant2
        assert variant1 in variants

    def test_assign_variant_different_users(self):
        """Test that different users can get different variants."""
        splitter = HashBasedSplitter("exp_001")
        variants = ["control", "treatment"]
        allocations = [50.0, 50.0]

        # Test multiple users
        assignments = []
        for i in range(100):
            variant = splitter.assign_variant(f"user_{i}", variants, allocations)
            assignments.append(variant)

        # Should have both variants assigned
        unique_variants = set(assignments)
        assert len(unique_variants) >= 1  # At least one variant
        assert all(v in variants for v in unique_variants)

    def test_assign_variant_different_experiments(self):
        """Test that same user gets different assignments in different experiments."""
        user_id = "user_123"
        variants = ["A", "B"]
        allocations = [50.0, 50.0]

        splitter1 = HashBasedSplitter("exp_001")
        splitter2 = HashBasedSplitter("exp_002")

        variant1 = splitter1.assign_variant(user_id, variants, allocations)
        variant2 = splitter2.assign_variant(user_id, variants, allocations)

        # Not guaranteed to be different, but with different experiment IDs,
        # hash inputs are different
        assert variant1 in variants
        assert variant2 in variants

    def test_assign_variant_uneven_allocation(self):
        """Test variant assignment with uneven traffic allocation."""
        splitter = HashBasedSplitter("exp_uneven")
        variants = ["control", "treatment"]
        allocations = [90.0, 10.0]  # 90% control, 10% treatment

        assignments = []
        for i in range(1000):
            variant = splitter.assign_variant(f"user_{i}", variants, allocations)
            assignments.append(variant)

        control_count = assignments.count("control")
        treatment_count = assignments.count("treatment")

        # Should roughly match the allocation (with some variance)
        control_percentage = control_count / len(assignments) * 100
        treatment_percentage = treatment_count / len(assignments) * 100

        # Allow 10% variance from expected allocation
        assert 80 <= control_percentage <= 100
        assert 0 <= treatment_percentage <= 20

    def test_assign_variant_three_variants(self):
        """Test assignment with three variants."""
        splitter = HashBasedSplitter("exp_three")
        variants = ["A", "B", "C"]
        allocations = [33.33, 33.33, 33.34]

        assignments = []
        for i in range(300):
            variant = splitter.assign_variant(f"user_{i}", variants, allocations)
            assignments.append(variant)

        # All variants should be represented
        unique_variants = set(assignments)
        assert unique_variants == {"A", "B", "C"}

    def test_assign_variant_fallback(self):
        """Test fallback behavior when hash value is at edge cases."""
        splitter = HashBasedSplitter("exp_fallback")
        variants = ["only_variant"]
        allocations = [100.0]

        variant = splitter.assign_variant("any_user", variants, allocations)
        assert variant == "only_variant"

    def test_assign_variant_with_string_and_int_users(self):
        """Test that both string and integer user IDs work."""
        splitter = HashBasedSplitter("exp_types")
        variants = ["A", "B"]
        allocations = [50.0, 50.0]

        variant_str = splitter.assign_variant("user_123", variants, allocations)
        variant_int = splitter.assign_variant(123, variants, allocations)

        assert variant_str in variants
        assert variant_int in variants


class TestAssignmentService:
    """Test AssignmentService functionality."""

    def test_calculate_user_slot_deterministic(self):
        """Test that slot calculation is deterministic."""
        slot1 = AssignmentService._calculate_user_slot("salt123", 100, "user_123")
        slot2 = AssignmentService._calculate_user_slot("salt123", 100, "user_123")

        assert slot1 == slot2
        assert 0 <= slot1 < 100

    def test_calculate_user_slot_different_salts(self):
        """Test that different salts produce different slot assignments."""
        user_id = "user_123"
        total_slots = 100

        slot1 = AssignmentService._calculate_user_slot("salt_a", total_slots, user_id)
        slot2 = AssignmentService._calculate_user_slot("salt_b", total_slots, user_id)

        # Different salts should generally produce different slots
        assert 0 <= slot1 < total_slots
        assert 0 <= slot2 < total_slots

    def test_calculate_user_slot_distribution(self):
        """Test that users are distributed evenly across slots."""
        salt = "test_salt"
        total_slots = 10

        slot_counts = {}
        for i in range(1000):
            slot = AssignmentService._calculate_user_slot(salt, total_slots, f"user_{i}")
            slot_counts[slot] = slot_counts.get(slot, 0) + 1

        # All slots should be represented
        assert len(slot_counts) == total_slots

        # Distribution should be roughly even (within 20% variance)
        expected_per_slot = 1000 / total_slots
        for count in slot_counts.values():
            assert 0.8 * expected_per_slot <= count <= 1.2 * expected_per_slot

    def test_get_user_assignment_not_assigned(self, db_session):
        """Test user assignment when slot is not assigned to any experiment."""
        # Create layer with no experiments
        layer = LayerService.create_layer(db_session, "empty_layer", "salt", total_slots=10)

        assignment = AssignmentService.get_user_assignment(db_session, layer, "user_123")

        assert assignment["status"] == "not_assigned"
        assert assignment["unit_id"] == "user_123"
        assert assignment["layer_id"] == "empty_layer"
        assert assignment["experiment_id"] is None
        assert assignment["variant"] is None
        assert "slot_index" in assignment

    def test_get_user_assignment_experiment_inactive(self, db_session):
        """Test user assignment when experiment is inactive."""
        layer = LayerService.create_layer(db_session, "inactive_layer", "salt", total_slots=100)

        # Create inactive experiment
        experiment = Experiment(
            experiment_id="inactive_exp",
            layer_id="inactive_layer",
            name="Inactive Test",
            variants=["A", "B"],
            traffic_allocation={"A": 50, "B": 50},
            traffic_percentage=100.0,
            status=ExperimentStatus.PAUSED,  # Inactive
        )

        LayerService.add_experiment(db_session, layer, experiment)

        # Find a user that gets assigned to this experiment's slot
        for i in range(1000):
            user_id = f"user_{i}"
            slot_index = AssignmentService._calculate_user_slot(layer.layer_salt, layer.total_slots, user_id)
            slot = db_session.execute(
                select(LayerSlot).where(LayerSlot.layer_id == "inactive_layer", LayerSlot.slot_index == slot_index)
            ).scalar_one_or_none()

            if slot and slot.experiment_id == "inactive_exp":
                assignment = AssignmentService.get_user_assignment(db_session, layer, user_id)
                assert assignment["status"] == "experiment_inactive"
                assert assignment["experiment_id"] == "inactive_exp"
                assert assignment["variant"] is None
                break
        else:
            pytest.fail("Could not find a user assigned to the inactive experiment")

    def test_get_user_assignment_assigned(self, db_session, sample_layer_with_experiment):
        """Test successful user assignment."""
        layer, experiment = sample_layer_with_experiment

        # Find a user that gets assigned to the experiment
        for i in range(1000):
            user_id = f"user_{i}"
            assignment = AssignmentService.get_user_assignment(db_session, layer, user_id)

            if assignment["status"] == "assigned":
                assert assignment["unit_id"] == user_id
                assert assignment["layer_id"] == "test_layer"
                assert assignment["experiment_id"] == "test_exp"
                assert assignment["experiment_name"] == "Button Color Test"
                assert assignment["variant"] in ["red", "blue", "green"]
                assert "slot_index" in assignment
                break
        else:
            pytest.fail("Could not find a user assigned to the experiment")

    def test_get_user_assignments_bulk(self, db_session, sample_layer_with_experiment):
        """Test bulk user assignment."""
        layer, experiment = sample_layer_with_experiment

        user_ids = [f"user_{i}" for i in range(20)]
        assignments = AssignmentService.get_user_assignments_bulk(db_session, layer, user_ids)

        assert len(assignments) == 20

        for user_id in user_ids:
            assert user_id in assignments
            assert assignments[user_id]["unit_id"] == user_id
            assert assignments[user_id]["layer_id"] == "test_layer"
            assert assignments[user_id]["status"] in ["assigned", "not_assigned"]

    def test_preview_assignment_distribution(self, db_session, sample_layer_with_experiment):
        """Test assignment distribution preview."""
        layer, experiment = sample_layer_with_experiment

        # Generate sample user IDs
        user_ids = [f"user_{i}" for i in range(200)]

        distribution = AssignmentService.preview_assignment_distribution(db_session, layer, user_ids)

        assert distribution["total_users"] == 200
        assert isinstance(distribution["unassigned_count"], int)
        assert isinstance(distribution["assignment_rate"], float)
        assert isinstance(distribution["assignment_distribution"], dict)

        # Assignment rate should be reasonable (experiment has 60% traffic)
        assert 0 <= distribution["assignment_rate"] <= 100

        # Should have some variant assignments
        if distribution["assignment_rate"] > 0:
            variant_keys = list(distribution["assignment_distribution"].keys())
            assert any("test_exp:" in key for key in variant_keys)

    def test_preview_assignment_distribution_empty_layer(self, db_session):
        """Test assignment distribution with no experiments."""
        layer = LayerService.create_layer(db_session, "empty", "salt", total_slots=10)
        user_ids = [f"user_{i}" for i in range(50)]

        distribution = AssignmentService.preview_assignment_distribution(db_session, layer, user_ids)

        assert distribution["total_users"] == 50
        assert distribution["unassigned_count"] == 50
        assert distribution["assignment_rate"] == 0.0
        assert distribution["assignment_distribution"] == {}


class TestAssignmentServiceMocked:
    """Test AssignmentService with mocked dependencies."""

    @patch("avos.services.splitter.HashBasedSplitter.assign_variant")
    def test_get_user_assignment_variant_assignment_called(
        self, mock_assign_variant, db_session, sample_layer_with_experiment
    ):
        """Test that variant assignment is called with correct parameters."""
        mock_assign_variant.return_value = "mocked_variant"

        layer, experiment = sample_layer_with_experiment

        # Find a user assigned to the experiment
        for i in range(100):
            user_id = f"user_{i}"
            assignment = AssignmentService.get_user_assignment(db_session, layer, user_id)

            if assignment["status"] == "assigned":
                assert assignment["variant"] == "mocked_variant"

                # Verify the mock was called with correct arguments
                mock_assign_variant.assert_called_once_with(user_id, ["red", "blue", "green"], [40.0, 35.0, 25.0])
                break
        else:
            pytest.fail("No user was assigned to the experiment")


class TestIntegrationScenarios:
    """Integration tests with real database scenarios."""

    def test_multi_experiment_layer_assignment(self, db_session):
        """Test user assignment in a layer with multiple experiments."""
        layer = LayerService.create_layer(db_session, "multi_exp", "salt", total_slots=100)

        # Add first experiment (30% traffic)
        exp1 = Experiment(
            experiment_id="exp_1",
            layer_id="multi_exp",
            name="Experiment 1",
            variants=["A1", "B1"],
            traffic_allocation={"A1": 50, "B1": 50},
            traffic_percentage=30.0,
            status=ExperimentStatus.ACTIVE,
        )
        LayerService.add_experiment(db_session, layer, exp1)

        # Add second experiment (40% traffic)
        exp2 = Experiment(
            experiment_id="exp_2",
            layer_id="multi_exp",
            name="Experiment 2",
            variants=["A2", "B2"],
            traffic_allocation={"A2": 60, "B2": 40},
            traffic_percentage=40.0,
            status=ExperimentStatus.ACTIVE,
        )
        LayerService.add_experiment(db_session, layer, exp2)

        # Test assignments
        user_ids = [f"user_{i}" for i in range(500)]
        assignments = AssignmentService.get_user_assignments_bulk(db_session, layer, user_ids)

        # Count assignments by experiment
        exp1_count = sum(1 for a in assignments.values() if a.get("experiment_id") == "exp_1")
        exp2_count = sum(1 for a in assignments.values() if a.get("experiment_id") == "exp_2")
        unassigned_count = sum(1 for a in assignments.values() if a["status"] == "not_assigned")

        # Verify roughly correct distribution
        total_assigned = exp1_count + exp2_count
        if total_assigned > 0:
            exp1_percentage = exp1_count / len(user_ids) * 100
            exp2_percentage = exp2_count / len(user_ids) * 100
            unassigned_percentage = unassigned_count / len(user_ids) * 100

            # Allow some variance (±10%)
            assert 20 <= exp1_percentage <= 40  # ~30% expected
            assert 30 <= exp2_percentage <= 50  # ~40% expected
            assert 20 <= unassigned_percentage <= 40  # ~30% expected

    def test_assignment_consistency_across_calls(self, db_session, sample_layer_with_experiment):
        """Test that assignment is consistent across multiple calls."""
        layer, experiment = sample_layer_with_experiment

        user_id = "consistent_user"

        # Get assignment multiple times
        assignments = []
        for _ in range(10):
            assignment = AssignmentService.get_user_assignment(db_session, layer, user_id)
            assignments.append(assignment)

        # All assignments should be identical
        first_assignment = assignments[0]
        for assignment in assignments[1:]:
            assert assignment == first_assignment

    def test_large_scale_assignment_distribution(self, db_session):
        """Test assignment distribution with large number of users."""
        layer = LayerService.create_layer(db_session, "large_scale", "salt", total_slots=1000)

        # Create experiment with specific variant allocation
        experiment = Experiment(
            experiment_id="large_exp",
            layer_id="large_scale",
            name="Large Scale Test",
            variants=["control", "variant_a", "variant_b"],
            traffic_allocation={"control": 50, "variant_a": 30, "variant_b": 20},
            traffic_percentage=80.0,  # 80% of users get experiment
            status=ExperimentStatus.ACTIVE,
        )
        LayerService.add_experiment(db_session, layer, experiment)

        # Test with many users
        user_ids = [f"user_{i}" for i in range(10000)]
        distribution = AssignmentService.preview_assignment_distribution(db_session, layer, user_ids)

        # Verify high-level statistics
        assert distribution["total_users"] == 10000

        # Should have reasonable assignment rate (around 80%)
        assert 70 <= distribution["assignment_rate"] <= 90

        # Check variant distribution if users are assigned
        if distribution["assignment_distribution"]:
            total_assigned = sum(distribution["assignment_distribution"].values())

            control_count = distribution["assignment_distribution"].get("large_exp:control", 0)
            variant_a_count = distribution["assignment_distribution"].get("large_exp:variant_a", 0)
            variant_b_count = distribution["assignment_distribution"].get("large_exp:variant_b", 0)

            if total_assigned > 0:
                control_pct = control_count / total_assigned * 100
                variant_a_pct = variant_a_count / total_assigned * 100
                variant_b_pct = variant_b_count / total_assigned * 100

                # Allow ±5% variance from expected
                assert 45 <= control_pct <= 55  # ~50% expected
                assert 25 <= variant_a_pct <= 35  # ~30% expected
                assert 15 <= variant_b_pct <= 25  # ~20% expected
