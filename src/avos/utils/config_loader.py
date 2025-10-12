import yaml
from avos.models.config_models import LayerConfig, ExperimentConfig


def load_layer_config(path):
    with open(path) as f:
        data = yaml.safe_load(f)
    return LayerConfig(**data)


def load_experiment_config(path):
    with open(path) as f:
        data = yaml.safe_load(f)
    return ExperimentConfig(**data)
