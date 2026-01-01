from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from avos.models.base import Base
from avos.services.assignment_service import AssignmentService
from avos.services.config_sync import apply_layer_configs
from avos.services.assignment_logger import LocalAssignmentLogger
from avos.services.layer_service import LayerService
from avos.srm_tester import SRMTester
from avos.utils.config_loader import load_layer_configs_from_dir

def apply_and_report(session, config_dir: Path, label: str) -> None:
    configs = load_layer_configs_from_dir(str(config_dir))
    apply_layer_configs(session, configs)
    print(f"\n== {label} ==")
    summarize_layers(session)


def summarize_layers(session) -> None:
    layers = LayerService.get_layers(session)
    if not layers:
        print("No layers")
        return
    for layer in layers:
        info = LayerService.get_layer_info(session, layer)
        print(f"Layer {info['layer_id']}")
        print(f"  total_slots: {info['total_slots']}")
        print(f"  used_slots: {info['used_slots']}")
        print(f"  free_slots: {info['free_slots']}")
        print(f"  active_experiments: {info['active_experiments']}")
        for exp_id, count in info["experiment_slot_counts"].items():
            print(f"  experiment {exp_id}: {count} slots")


def show_assignments(session, unit_id: str) -> None:
    layers = LayerService.get_layers(session)
    for layer in layers:
        assignment = AssignmentService.assign_for_layer(session, layer, unit_id)
        print(
            f"assignment unit_id={unit_id} layer_id={assignment['layer_id']} "
            f"status={assignment['status']} experiment_id={assignment['experiment_id']} "
            f"variant={assignment['variant']}"
        )


def show_metrics(session, sample_unit_ids) -> None:
    layers = LayerService.get_layers(session)
    tester = SRMTester()
    for layer in layers:
        metrics = AssignmentService.preview_assignment_metrics(session, layer, sample_unit_ids, srm_tester=tester)
        print(f"metrics layer_id={layer.layer_id} assignment_rate={metrics['assignment_rate']}")
        for exp_id, result in metrics.get("srm_results", {}).items():
            print(f"  srm {exp_id}: {result}")


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    config_root = root / "examples" / "configs"
    config_v1 = config_root / "v1"
    config_v2 = config_root / "v2"

    engine = create_engine("sqlite:///examples/workflow.db", echo=False)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        apply_and_report(session, config_v1, "apply v1 configs")
        show_assignments(session, "user_123")
        show_metrics(session, [f"user_{i}" for i in range(200)])

        logger = LocalAssignmentLogger("examples/workflow_assignments.duckdb")
        try:
            layer = LayerService.get_layer(session, "homepage_hero")
            if layer:
                AssignmentService.assign_for_layer(session, layer, "user_456", assignment_logger=logger)
                count = logger.con.execute("SELECT COUNT(*) FROM user_assignments").fetchone()[0]
                print(f"logged_assignments={count}")
        finally:
            logger.close()

        apply_and_report(session, config_v2, "apply v2 configs")
        show_assignments(session, "user_123")
        show_metrics(session, [f"user_{i}" for i in range(200)])
    finally:
        session.close()


if __name__ == "__main__":
    main()
