from pathlib import Path
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


def load_layer_configs_from_dir(path: str) -> list[LayerConfig]:
    root = Path(path)
    if not root.exists():
        raise FileNotFoundError(f"Config directory not found: {path}")
    if not root.is_dir():
        raise ValueError(f"Config path is not a directory: {path}")

    files = sorted([*root.glob("*.yml"), *root.glob("*.yaml")])
    configs: list[LayerConfig] = []
    for config_path in files:
        configs.append(load_layer_config(config_path))
    return configs
