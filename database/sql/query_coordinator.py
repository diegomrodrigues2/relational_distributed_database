import json
import re
from concurrent import futures
from ..replication.replica import replication_pb2

class QueryCoordinator:
    """Execute distributed SELECT queries using scatter-gather."""

    def __init__(self, nodes):
        self.nodes = list(nodes)

    def _parse_table(self, sql: str) -> str:
        try:
            from .parser import parse_sql

            q = parse_sql(sql)
            return q.from_clause.table
        except Exception:
            m = re.search(r"FROM\s+(\w+)", sql, re.IGNORECASE)
            if not m:
                raise
            return m.group(1)

    def execute(self, sql: str):
        """Execute ``sql`` across all nodes and return combined rows."""
        table = self._parse_table(sql)
        plan = json.dumps({"table": table})
        request = replication_pb2.PlanRequest(plan=plan)

        def _call(node):
            rows = []
            try:
                for resp in node.client.stub.ExecutePlan(request):
                    rows.append(json.loads(resp.data))
            except Exception:
                pass
            return rows

        results = []
        with futures.ThreadPoolExecutor(max_workers=len(self.nodes)) as ex:
            for rows in ex.map(_call, self.nodes):
                results.extend(rows)
        return results

