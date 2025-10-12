from avos.utils.config_loader import load_experiment_config, load_layer_config


def test_load_experiment(tmp_path):
    yaml_content = """
        experiment_id: exp7
        layer_id: layer123
        name: New Button Test
        variants: [A, B, C]
        traffic_allocation: {A: 33, B: 33, C: 34}
        status: active
    """
    yaml_file = tmp_path / "experiment.yaml"
    yaml_file.write_text(yaml_content)
    from avos.utils.config_loader import load_experiment_config

    cfg = load_experiment_config(str(yaml_file))
    assert cfg.experiment_id == "exp7"
    assert cfg.variants == ["A", "B", "C"]


def test_load_layer(tmp_path):
    yaml_content = """
        layer_id: layer123
        layer_salt: test_salt
        total_slots: 50
        total_traffic_percentage: 95.0
    """
    yaml_file = tmp_path / "layer.yaml"
    yaml_file.write_text(yaml_content)
    from avos.utils.config_loader import load_layer_config

    cfg = load_layer_config(str(yaml_file))
    assert cfg.layer_id == "layer123"
    assert cfg.total_slots == 50
    assert cfg.total_traffic_percentage == 95.0
