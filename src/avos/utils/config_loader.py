import yaml
from avos.models.config_models import LayerConfig

def load_layer_config(path):
    with open(path) as f:
        data = yaml.safe_load(f)
    return LayerConfig(**data)

def load_experiment_configs(path):
    with open(path) as f:
        data = yaml.safe_load(f)
    return [ExperimentConfig(**exp) for exp in data['experiments']]
