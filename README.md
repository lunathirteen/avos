<div align="center">
  <img src="./assets/avos_logo.png" alt="АВОСЬ Logo" width="200"/>
  <h1>АВОСЬ</h1>
  <p>Open source A/B testing platform with a touch of serendipity</p>
</div>

## Overview

AVOS is a lightweight A/B experimentation core with:
- deterministic hashing splitters
- layer/slot allocation for traffic control
- YAML-driven configuration with validation
- config sync workflow (apply configs to DB)

## Quickstart

Use `uv` to run examples/tests:

```bash
uv run python examples/service_workflow.py
uv run pytest tests/
```

The example workflow reads YAML configs, applies them to a SQLite DB, and prints layer utilization and assignments.

## YAML Config Workflow

Configs live in a directory (one file per layer). Apply them to the DB via the config sync API:

```python
from avos.services.config_sync import apply_layer_configs
from avos.utils.config_loader import load_layer_configs_from_dir

configs = load_layer_configs_from_dir("examples/configs/v1")
apply_layer_configs(session, configs)
```

Example directory:

```
examples/
  configs/
    v1/
      homepage_hero.yml
      checkout_flow.yml
    v2/
      homepage_hero.yml
      checkout_flow.yml
```

Example layer config:

```yaml
layer_id: homepage_hero
layer_salt: homepage_salt_2025
total_slots: 100
total_traffic_percentage: 1.0
experiments:
  - experiment_id: hero_button_colors_v1
    layer_id: homepage_hero
    name: Hero Button Colors v1
    variants: [blue, green]
    traffic_allocation: {blue: 0.5, green: 0.5}
    status: active
    splitter_type: hash
    traffic_percentage: 0.6
```

## Config Rules (Validation)

- `traffic_allocation` must sum to `1.0`, keys must match `variants`
- `variants` must be unique and non-empty
- `segment_allocations`/`geo_allocations`/`stratum_allocations` require matching `splitter_type`
- `traffic_percentage` must be between `0` and `1`
- `traffic_percentage` can only increase for an existing experiment (ramp up)
- `start_date` must be before `end_date` if both are set
- `total_slots` must be positive; `total_traffic_percentage` is `0 < x <= 1`

## Sync Rules (Safety)

- Experiments are **not** deleted implicitly. To remove, set `status: completed`
- `variants`, `splitter_type`, and `layer_id` are immutable after creation
- Allocation changes require a new experiment; winner rollout (one variant `1.0`, others `0.0`) is allowed only when `status: completed`
- Completed experiments cannot be modified

## Notes

- Assignments are deterministic for hash-based splitters.
- Segment/geo/stratum allocations use the same `variants` keys and `0–1` fractions.

## Observability

Log assignments with a local logger that implements `log_assignments(assignments)`:

```python
from avos.services.assignment_logger import LocalAssignmentLogger
from avos.services.assignment_service import AssignmentService

logger = LocalAssignmentLogger("avos_assignments.duckdb")
assignment = AssignmentService.assign_for_layer(session, layer, "user_123", assignment_logger=logger)
```

Preview assignment metrics with SRM checks:

```python
from avos.srm_tester import SRMTester

metrics = AssignmentService.preview_assignment_metrics(
    session, layer, sample_unit_ids, srm_tester=SRMTester()
)
```
