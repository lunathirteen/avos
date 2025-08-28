import pytest
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from avos.models.experiment import Base, Experiment, ExperimentStatus
from avos.models.layer import Layer


@pytest.fixture
def db_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.rollback()
    session.close()


def valid_data():
    return dict(
        experiment_id="exp1",
        name="My AB Test",
        variants=["control", "treatment"],
        traffic_allocation={"control": 60, "treatment": 40},
        traffic_percentage=100.0,
        start_date=datetime.now() - timedelta(days=2),
        end_date=datetime.now() + timedelta(days=2),
        status=ExperimentStatus.ACTIVE,
        layer_id="layer1",
        priority=3,
    )

def test_experiment_create_and_layer_link(db_session):
    # Create a layer
    layer = Layer(layer_id="layer1", layer_salt="salt")
    db_session.add(layer)
    db_session.commit()

    exp = Experiment(**valid_data())
    db_session.add(exp)
    db_session.commit()

    found = db_session.query(Experiment).filter_by(experiment_id="exp1").first()
    assert found.layer_id == "layer1"
    assert found.layer.layer_id == "layer1"
    assert found.is_active(datetime.now())
    assert "control" in found.variant_list()
    assert abs(found.traffic_dict()["control"] - 60) < 0.01


def test_experiment_create_and_query(db_session):
    exp = Experiment(**valid_data())
    db_session.add(exp)
    db_session.commit()
    found = db_session.query(Experiment).filter_by(experiment_id="exp1").first()
    assert found.name == "My AB Test"
    assert found.is_active(datetime.now())
    assert "control" in found.variant_list()
    assert abs(found.traffic_dict()["control"] - 60) < 0.01


def test_active_status_logic(db_session):
    data = valid_data()
    data["status"] = ExperimentStatus.PAUSED
    exp = Experiment(**data)
    db_session.add(exp)
    db_session.commit()
    found = db_session.query(Experiment).filter_by(experiment_id="exp1").first()
    assert not found.is_active(datetime.now())


def test_before_start_date_inactive():
    data = valid_data()
    data["start_date"] = datetime.now() + timedelta(days=1)
    exp = Experiment(**data)
    assert not exp.is_active(datetime.now())


def test_after_end_date_inactive():
    data = valid_data()
    data["end_date"] = datetime.now() - timedelta(days=1)
    exp = Experiment(**data)
    assert not exp.is_active(datetime.now())


def test_bad_traffic_allocation_negative():
    data = valid_data()
    data["traffic_allocation"] = {"control": 100, "treatment": -10}
    with pytest.raises(ValueError):
        Experiment(**data)


def test_bad_traffic_allocation_sum():
    data = valid_data()
    data["traffic_allocation"] = {"control": 10, "treatment": 10}
    with pytest.raises(ValueError):
        Experiment(**data)


def test_bad_traffic_percentage():
    data = valid_data()
    data["traffic_percentage"] = 101
    with pytest.raises(ValueError):
        Experiment(**data)


def test_bad_dates():
    data = valid_data()
    data["start_date"] = datetime.now() + timedelta(days=2)
    data["end_date"] = datetime.now() + timedelta(days=1)
    with pytest.raises(ValueError):
        Experiment(**data)
