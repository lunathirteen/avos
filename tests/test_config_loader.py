import pytest
from avos.utils.config_loader import load_layer_config

def test_load_valid_layer_config():
    data = {
        "layer_id": "layer1",
        "layer_salt": "abc123",
        "total_slots": 100,
        "total_traffic_percentage": 100.0,
        "experiments": [{
            "experiment_id": "exp1",
            "name": "Homepage CTA",
            "status": "active",
            "variants": ["A", "B"],
            "traffic_allocation": {"A": 60, "B": 40},
            "start_date": "2025-10-01T10:00:00Z",
            "end_date": "2025-10-31T10:00:00Z"
        }]
    }
    from avos.models.config_models import LayerConfig
    config = LayerConfig(**data)
    assert config.layer_id == "layer1"
    assert config.experiments[0].status == "active"
