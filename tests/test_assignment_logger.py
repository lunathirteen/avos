from avos.services.assignment_logger import InMemoryAssignmentLogger


def test_in_memory_assignment_logger():
    logger = InMemoryAssignmentLogger()
    try:
        assignments = [
            {
                "unit_id": "u1",
                "layer_id": "layer1",
                "slot_index": 1,
                "experiment_id": "exp1",
                "experiment_name": "Experiment One",
                "variant": "A",
                "status": "assigned",
            },
            {
                "unit_id": "u2",
                "layer_id": "layer1",
                "slot_index": 2,
                "experiment_id": "exp1",
                "experiment_name": "Experiment One",
                "variant": "B",
                "status": "assigned",
            },
        ]
        logger.log_assignments(assignments)
        count = logger.con.execute("SELECT COUNT(*) FROM user_assignments").fetchone()[0]
        assert count == 2
    finally:
        logger.close()
