"""Run a .sql file statement-by-statement on a SQL warehouse: python3 run_sql.py sql/setup_uc.sql"""
import os
import sys

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

w = WorkspaceClient(profile=sys.argv[2] if len(sys.argv) > 2 else "DEFAULT")
wh = next(x for x in w.warehouses.list() if x.state and x.state.value in ("RUNNING", "STARTING", "STOPPED"))
sql = open(sys.argv[1]).read().replace("${catalog}", os.environ.get("DEMO_CATALOG", "main"))
lines = [l for l in sql.splitlines() if not l.strip().startswith("--")]
for stmt in [s.strip() for s in "\n".join(lines).split(";") if s.strip()]:
    r = w.statement_execution.execute_statement(statement=stmt, warehouse_id=wh.id, wait_timeout="50s")
    while r.status.state in (StatementState.PENDING, StatementState.RUNNING):
        r = w.statement_execution.get_statement(r.statement_id)
    print(f"[{r.status.state.value}] {stmt.splitlines()[0][:90]}")
    if r.status.state != StatementState.SUCCEEDED:
        sys.exit(r.status.error.message)
