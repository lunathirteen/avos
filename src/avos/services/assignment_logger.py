import duckdb
from datetime import datetime
from typing import List, Dict, Any


class DuckDBAssignmentLogger:
    def __init__(self, db_path: str = "assignments.duckdb"):
        self.con = duckdb.connect(database=db_path)
        self._initialize_table()

    def _initialize_table(self):
        self.con.execute("""
        CREATE TABLE IF NOT EXISTS user_assignments (
            unit_id VARCHAR,
            layer_id VARCHAR,
            slot_index INTEGER,
            experiment_id VARCHAR,
            experiment_name VARCHAR,
            variant VARCHAR,
            status VARCHAR,
            assignment_timestamp TIMESTAMP
        )
        """)

    def log_assignments(self, assignments: List[Dict[str, Any]]):
        records = []
        now = datetime.utcnow()
        for assignment in assignments:
            records.append((
                assignment["unit_id"],
                assignment["layer_id"],
                assignment["slot_index"],
                assignment.get("experiment_id"),
                assignment.get("experiment_name"),
                assignment.get("variant"),
                assignment["status"],
                now,
            ))
        self.con.executemany(
            """
            INSERT INTO user_assignments
            (unit_id, layer_id, slot_index, experiment_id, experiment_name, variant, status, assignment_timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            records
        )

    def close(self):
        self.con.close()
