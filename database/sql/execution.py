from __future__ import annotations

import base64
import heapq
import os
from typing import Iterator, Iterable, List

from .ast import Column, Literal, BinOp, Expression
from .serialization import RowSerializer
from ..lsm.lsm_db import SimpleLSMDB
from ..lsm.sstable import TOMBSTONE
from ..utils.vector_clock import VectorClock

class PlanNode:
    """Abstract execution plan node."""

    def execute(self) -> Iterator[dict]:
        raise NotImplementedError


def _merge_version_lists(current: list, new_list: list) -> list:
    """Merge version tuples using vector clocks."""
    if not current:
        return list(new_list)
    result = list(current)
    for item in new_list:
        val, vc = item[0], item[1]
        created = item[2] if len(item) > 2 else None
        deleted = item[3] if len(item) > 3 else None
        add_new = True
        updated = []
        for cur in result:
            c_val, c_vc = cur[0], cur[1]
            c_created = cur[2] if len(cur) > 2 else None
            c_deleted = cur[3] if len(cur) > 3 else None
            cmp = vc.compare(c_vc)
            if cmp == ">":
                continue
            if cmp == "<":
                add_new = False
                updated.append((c_val, c_vc, c_created, c_deleted))
            else:
                if (
                    vc.clock == c_vc.clock
                    and val == c_val
                    and created == c_created
                    and deleted == c_deleted
                ):
                    add_new = False
                updated.append((c_val, c_vc, c_created, c_deleted))
        if add_new:
            updated.append((val, vc, created, deleted))
        result = updated
    return result


class MergingIterator:
    """Merge multiple sorted iterators of key/value tuples."""

    def __init__(self, *iterables: Iterable[tuple[str, str, VectorClock]]):
        self.heap: list[tuple[str, str, VectorClock, Iterator]] = []
        for it in iterables:
            it = iter(it)
            try:
                k, v, vc = next(it)
                heapq.heappush(self.heap, (k, v, vc, it))
            except StopIteration:
                continue
        self.current_key: str | None = None
        self.current_versions: list[tuple[str, VectorClock]] = []

    def __iter__(self):
        return self

    def __next__(self):
        while self.heap:
            k, v, vc, it = heapq.heappop(self.heap)
            try:
                nxt = next(it)
                heapq.heappush(self.heap, (nxt[0], nxt[1], nxt[2], it))
            except StopIteration:
                pass
            if self.current_key is None:
                self.current_key = k
                self.current_versions = _merge_version_lists([], [(v, vc)])
                continue
            if k == self.current_key:
                self.current_versions = _merge_version_lists(
                    self.current_versions, [(v, vc)]
                )
                continue
            # emit previous key
            result_versions = [r for r in self.current_versions if r[0] != TOMBSTONE]
            result_key = self.current_key
            self.current_key = k
            self.current_versions = _merge_version_lists([], [(v, vc)])
            if result_versions:
                return result_key, result_versions
        if self.current_key is not None:
            result_versions = [r for r in self.current_versions if r[0] != TOMBSTONE]
            result_key = self.current_key
            self.current_key = None
            self.current_versions = []
            if result_versions:
                return result_key, result_versions
        raise StopIteration


def _eval_expr(row: dict, expr: Expression) -> object:
    if isinstance(expr, Literal):
        return expr.value
    if isinstance(expr, Column):
        return row.get(expr.name)
    if isinstance(expr, BinOp):
        left = _eval_expr(row, expr.left)
        right = _eval_expr(row, expr.right)
        if expr.op == "AND":
            return bool(left) and bool(right)
        if expr.op == "OR":
            return bool(left) or bool(right)
        if expr.op == "EQ":
            return left == right
        if expr.op == "NEQ":
            return left != right
        if expr.op == "GT":
            return left > right
        if expr.op == "GTE":
            return left >= right
        if expr.op == "LT":
            return left < right
        if expr.op == "LTE":
            return left <= right
        raise ValueError(f"unknown operator {expr.op}")
    raise ValueError(f"unsupported expression {type(expr)!r}")


class SeqScanNode(PlanNode):
    """Sequential scan over a table."""

    def __init__(
        self,
        db: SimpleLSMDB,
        table: str,
        where_clause: Expression | None = None,
        columns: List[str] | None = None,
    ) -> None:
        self.db = db
        self.table = table
        self.where_clause = where_clause
        self.columns = columns

    def _iterators(self) -> list[Iterable[tuple[str, str, VectorClock]]]:
        prefix = f"{self.table}||"
        iters = []
        iters.append(
            [i for i in self.db.get_segment_items("memtable") if i[0].startswith(prefix)]
        )
        with self.db.sstable_manager._segments_lock:
            segments = list(self.db.sstable_manager.sstable_segments)
        for _ts, path, _index in segments:
            seg_id = os.path.basename(path)
            items = [
                i
                for i in self.db.get_segment_items(seg_id)
                if i[0].startswith(prefix)
            ]
            iters.append(items)
        return iters

    def execute(self) -> Iterator[dict]:
        iterators = [iter(it) for it in self._iterators()]
        merging = MergingIterator(*iterators)
        for _key, versions in merging:
            if not versions:
                continue
            value = versions[0][0]
            try:
                decoded = base64.b64decode(value)
            except Exception:
                decoded = value.encode() if isinstance(value, str) else value
            try:
                row = RowSerializer.loads(decoded)
            except Exception:
                continue
            if self.where_clause is not None:
                try:
                    ok = bool(_eval_expr(row, self.where_clause))
                except Exception:
                    ok = False
                if not ok:
                    continue
            if self.columns:
                yield {c: row.get(c) for c in self.columns}
            else:
                yield row


class IndexScanNode(PlanNode):
    """Scan rows using a secondary index."""

    def __init__(
        self,
        db: SimpleLSMDB,
        index_manager,
        table_name: str,
        index_name: str,
        lookup_value,
        columns: List[str] | None = None,
    ) -> None:
        self.db = db
        self.index_manager = index_manager
        self.table_name = table_name
        self.index_name = index_name
        self.lookup_value = lookup_value
        self.columns = columns

    def execute(self) -> Iterator[dict]:
        keys = self.index_manager.query(self.index_name, self.lookup_value, self.table_name)
        for key in keys:
            records = self.db.get_record(key)
            if not records:
                continue
            value = records[0][0]
            try:
                decoded = base64.b64decode(value)
            except Exception:
                decoded = value.encode() if isinstance(value, str) else value
            try:
                row = RowSerializer.loads(decoded)
            except Exception:
                continue
            if self.columns:
                yield {c: row.get(c) for c in self.columns}
            else:
                yield row


class NestedLoopJoinNode(PlanNode):
    """Batched nested loop join implementation."""

    def __init__(
        self,
        outer_plan: PlanNode,
        inner_plan_builder,
        outer_key: str,
        inner_key: str,
        batch_size: int = 100,
    ) -> None:
        self.outer_plan = outer_plan
        self.inner_plan_builder = inner_plan_builder
        self.outer_key = outer_key
        self.inner_key = inner_key
        self.batch_size = batch_size

    def execute(self) -> Iterator[dict]:
        outer_iter = iter(self.outer_plan.execute())
        while True:
            batch: list[dict] = []
            for _ in range(self.batch_size):
                try:
                    batch.append(next(outer_iter))
                except StopIteration:
                    break
            if not batch:
                break

            key_set = {row.get(self.outer_key) for row in batch}
            key_set.discard(None)

            inner_rows_by_key: dict[object, list[dict]] = {}
            inner_plan = self.inner_plan_builder()
            for row in inner_plan.execute():
                val = row.get(self.inner_key)
                if val in key_set:
                    inner_rows_by_key.setdefault(val, []).append(row)

            for o in batch:
                val = o.get(self.outer_key)
                matches = inner_rows_by_key.get(val) or []
                for i in matches:
                    yield {**o, **i}
