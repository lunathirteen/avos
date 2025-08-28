from datetime import datetime, timedelta

from avos.db_config import get_session
from avos.models.layer import Layer
from avos.models.experiment import Experiment, ExperimentStatus
from avos.services.layer_service import LayerService

sess = get_session("sqlite:///demo.db")

# create a layer
layer = LayerService.create_layer(sess, "layer1", "salt123", total_slots=100)

# add an experiment
exp = Experiment(
    experiment_id="expA",
    name="Homepage CTA Test",
    variants=["control", "red"],
    traffic_allocation={"control": 50, "red": 50},
    traffic_percentage=30,
    start_date=datetime.now(),
    end_date=datetime.now() + timedelta(days=14),
    status=ExperimentStatus.ACTIVE,
    layer_id="layer1",
)
ok = LayerService.add_experiment(sess, layer, exp)
print("Added:", ok)

# user assignment
result = LayerService.get_user_assignment(sess, layer, unit_id="user_42")
print(result)

# layer stats
print(LayerService.layer_info(sess, layer))
