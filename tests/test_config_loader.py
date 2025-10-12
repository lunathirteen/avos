import pytest
import json
from avos.utils.config_loader import load_layer_config, config_to_orm
from avos.models.config_models import LayerConfig
from avos.models.experiment import Experiment
from avos.models.layer import Layer

def test_full_layer_loader(tmp_path):
    # Step 1: Create a sample YAML config file
    yaml_content = """
        layer_id: layer1
        layer_salt: abc123
        total_slots: 100
        total_traffic_percentage: 100.0
        experiments:
        - experiment_id: exp1
            name: Homepage CTA Test
            layer_id: layer1
            status: active
            variants: [A, B]
            traffic_allocation: {A: 60, B: 40}
            traffic_percentage: 50.0
            start_date: "2025-10-01T10:00:00Z"
            end_date: "2025-10-31T10:00:00Z"
            priority: 1
        - experiment_id: exp2
            name: Onboarding Test
            layer_id: layer1
            status: draft
            variants: [X, Y]
            traffic_allocation: {X: 50, Y: 50}
            traffic_percentage: 50.0
            start_date: "2025-11-01T00:00:00Z"
            end_date: "2025-11-15T00:00:00Z"
            priority: 1
        """
    yaml_file = tmp_path / "layer.yaml"
    yaml_file.write_text(yaml_content)

    # Step 2: Load and validate config using loader
    layer_config = load_layer_config(str(yaml_file))
    assert isinstance(layer_config, LayerConfig)
    assert layer_config.layer_id == "layer1"
    assert len(layer_config.experiments) == 2
    assert layer_config.experiments[0].name == "Homepage CTA Test"
    assert layer_config.experiments[1].variants == ["X", "Y"]

    # Step 3: Map to ORM Layer (with embedded ORM Experiments)
    layer = config_to_orm(layer_config)
    assert isinstance(layer, Layer)
    assert layer.layer_id == "layer1"
    assert len(layer.experiments) == 2

    exp1 = layer.experiments[0]
    assert isinstance(exp1, Experiment)
    assert exp1.name == "Homepage CTA Test"
    assert json.loads(exp1.variants) == ["A", "B"]
    assert json.loads(exp1.traffic_allocation)["A"] == 60

    exp2 = layer.experiments[1]
    assert exp2.status == "draft"
    assert json.loads(exp2.variants) == ["X", "Y"]
    assert json.loads(exp2.traffic_allocation) == {"X": 50, "Y": 50}
    assert exp2.layer_id == "layer1"
