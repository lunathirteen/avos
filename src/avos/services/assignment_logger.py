import duckdb
from datetime import datetime
from typing import List, Dict, Any
import os  # For environment variable handling


class MotherDuckAssignmentLogger:
    def __init__(self, db_name: str = "avos_db", token: str = None):
        # Set up authentication (use env var for security in production)
        if token is None:
            token = os.getenv("MOTHERDUCK_TOKEN")
        if not token:
            raise ValueError("MotherDuck token required. Set via env var or constructor.")

        # Connect to MotherDuck (equivalent to 'duckdb:///md:avos_db')
        self.con = duckdb.connect(f"md:{db_name}?motherduck_token={token}")

        # Initialize table (TIMESTAMP handles datetime objects directly)
        self._initialize_table()

    def _initialize_table(self):
        self.con.execute(
            """
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
            """
        )

    def log_assignments(self, assignments: List[Dict[str, Any]]):
        records = []
        now = datetime.utcnow()  # UTC for consistency across regions
        for assignment in assignments:
            records.append(
                (
                    assignment["unit_id"],
                    assignment["layer_id"],
                    assignment["slot_index"],
                    assignment.get("experiment_id"),
                    assignment.get("experiment_name"),
                    assignment.get("variant"),
                    assignment["status"],
                    now,  # Inserted directly; DuckDB converts to TIMESTAMP
                )
            )
        self.con.executemany(
            """
            INSERT INTO user_assignments
            (unit_id, layer_id, slot_index, experiment_id, experiment_name, variant, status, assignment_timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            records,
        )

    def close(self):
        self.con.close()
