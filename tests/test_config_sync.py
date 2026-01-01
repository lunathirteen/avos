import pytest
from sqlalchemy import create_engine, select, func
from sqlalchemy.orm import sessionmaker

from avos.models.base import Base
from avos.models.experiment import ExperimentStatus
from avos.models.layer import LayerSlot
from avos.models.config_models import LayerConfig, ExperimentConfig
from avos.services.config_sync import apply_layer_configs
from avos.services.layer_service import LayerService


@pytest.fixture
def db_session():
    engine = create_engine("sqlite:///:memory:", echo=False)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.rollback()
    session.close()


def test_apply_layer_configs_creates_layer_and_experiment(db_session):
    layer_config = LayerConfig(
        layer_id="layer_sync",
        layer_salt="salt_sync",
        total_slots=10,
        total_traffic_percentage=100.0,
        experiments=[
            ExperimentConfig(
                experiment_id="exp_sync",
                layer_id="layer_sync",
                name="Sync Test",
                variants=["A", "B"],
                traffic_allocation={"A": 0.5, "B": 0.5},
                status="active",
                traffic_percentage=50.0,
            )
        ],
    )

    apply_layer_configs(db_session, [layer_config])

    experiment = LayerService.get_experiment(db_session, "exp_sync")
    assert experiment is not None
    assert experiment.status == ExperimentStatus.ACTIVE

    allocated_slots = db_session.execute(
        select(func.count())
        .select_from(LayerSlot)
        .where(LayerSlot.layer_id == "layer_sync", LayerSlot.experiment_id == "exp_sync")
    ).scalar()
    assert allocated_slots == 5


def test_apply_layer_configs_completed_frees_slots(db_session):
    layer_config = LayerConfig(
        layer_id="layer_sync",
        layer_salt="salt_sync",
        total_slots=10,
        total_traffic_percentage=100.0,
        experiments=[
            ExperimentConfig(
                experiment_id="exp_sync",
                layer_id="layer_sync",
                name="Sync Test",
                variants=["A", "B"],
                traffic_allocation={"A": 0.5, "B": 0.5},
                status="active",
                traffic_percentage=50.0,
            )
        ],
    )
    apply_layer_configs(db_session, [layer_config])

    completed_config = LayerConfig(
        layer_id="layer_sync",
        layer_salt="salt_sync",
        total_slots=10,
        total_traffic_percentage=100.0,
        experiments=[
            ExperimentConfig(
                experiment_id="exp_sync",
                layer_id="layer_sync",
                name="Sync Test",
                variants=["A", "B"],
                traffic_allocation={"A": 0.5, "B": 0.5},
                status="completed",
                traffic_percentage=50.0,
            )
        ],
    )

    apply_layer_configs(db_session, [completed_config])

    experiment = LayerService.get_experiment(db_session, "exp_sync")
    assert experiment.status == ExperimentStatus.COMPLETED

    allocated_slots = db_session.execute(
        select(func.count())
        .select_from(LayerSlot)
        .where(LayerSlot.layer_id == "layer_sync", LayerSlot.experiment_id == "exp_sync")
    ).scalar()
    assert allocated_slots == 0


def test_apply_layer_configs_variants_change_rejected(db_session):
    layer_config = LayerConfig(
        layer_id="layer_sync",
        layer_salt="salt_sync",
        total_slots=10,
        total_traffic_percentage=100.0,
        experiments=[
            ExperimentConfig(
                experiment_id="exp_sync",
                layer_id="layer_sync",
                name="Sync Test",
                variants=["A", "B"],
                traffic_allocation={"A": 0.5, "B": 0.5},
                status="active",
            )
        ],
    )
    apply_layer_configs(db_session, [layer_config])

    changed = LayerConfig(
        layer_id="layer_sync",
        layer_salt="salt_sync",
        total_slots=10,
        total_traffic_percentage=100.0,
        experiments=[
            ExperimentConfig(
                experiment_id="exp_sync",
                layer_id="layer_sync",
                name="Sync Test",
                variants=["A", "C"],
                traffic_allocation={"A": 0.5, "C": 0.5},
                status="active",
            )
        ],
    )

    with pytest.raises(ValueError, match="variants cannot be changed"):
        apply_layer_configs(db_session, [changed])
