# Executor Quick Reference Card

**PostgreSQL 17.6 -- 2-Page Summary**

---

## Key Concepts

| Concept | One-Liner |
|---------|-----------|
| **Volcano Model** | Each node has a `next-tuple` function; parent pulls from children |
| **PlanState Tree** | Runtime mirror of the Plan tree; built by `ExecInitNode` |
| **ExprState** | Compiled expression: flat step array + evalfunc pointer |
| **TupleTableSlot** | Universal tuple container with virtual method dispatch |
| **ExprContext** | Tuple slots + memory contexts for expression evaluation |
| **EState** | Per-query shared state: snapshots, range table, parameters |
| **QueryDesc** | Bridge between planner output and executor |

## Execution Lifecycle

```
ExecutorStart(queryDesc, eflags)
    --> CreateExecutorState()
    --> InitPlan() --> ExecInitNode() [recursive]

ExecutorRun(queryDesc, direction, count, execute_once)
    --> ExecutePlan()
        loop: ExecProcNode(root) --> dest->receiveSlot(tuple)

ExecutorFinish(queryDesc)
    --> AfterTriggerEndQuery() [fire AFTER triggers]

ExecutorEnd(queryDesc)
    --> ExecEndNode() [recursive cleanup]
    --> FreeExecutorState()
```

## Critical Functions

| Function | Purpose |
|----------|---------|
| `ExecInitNode(plan, estate, eflags)` | Dispatch: Plan tree to PlanState tree |
| `ExecProcNode(planstate)` | Pull next tuple from any node |
| `ExecEndNode(planstate)` | Recursive cleanup of all nodes |
| `ExecReScan(planstate)` | Reset node for rescanning |
| `ExecScan(scanstate, accessMtd, recheckMtd)` | Generic scan loop: fetch + qual + project |
| `ExecQual(exprstate, econtext)` | Evaluate WHERE/JOIN condition (bool) |
| `ExecProject(projInfo)` | Compute output columns into result slot |
| `ExecInitExpr(expr, parent)` | Compile expression tree to ExprState |

## Node Type Categories (43 total)

| Category | Count | Examples |
|----------|-------|---------|
| Control | 8 | Result, Append, ModifyTable, RecursiveUnion |
| Scan | 16 | SeqScan, IndexScan, BitmapHeapScan, ForeignScan |
| Join | 3 | NestLoop, MergeJoin, HashJoin |
| Material/Sort | 4 | Sort, Material, Memoize, IncrementalSort |
| Aggregation | 5 | Agg, WindowAgg, Group, Unique, SetOp |
| Parallel | 2 | Gather, GatherMerge |
| Lock/Limit | 2 | LockRows, Limit |
| Auxiliary | 2 | Hash, SubPlan |

---

## Common Debugging Tips

### EXPLAIN ANALYZE Interpretation

```sql
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) SELECT ...;
```

| EXPLAIN Field | What It Means |
|---------------|---------------|
| `actual time=X..Y` | X = time to first tuple; Y = time to last tuple (ms) |
| `rows=N` | Actual number of rows returned by this node |
| `loops=N` | Number of times this node was executed (NestLoop inner) |
| `Rows Removed by Filter: N` | Tuples rejected by WHERE/qual |
| `Rows Removed by Join Filter: N` | `InstrCountFiltered1` (joinqual fail) |
| `Buffers: shared hit=X read=Y` | Buffer pool hits vs disk reads |
| `Sort Method: top-N heapsort` | Optimized sort for LIMIT queries |
| `Batches: N` | Hash join used N batches (>1 means overflow) |
| `Peak Memory Usage: N kB` | Maximum memory used by sort/hash |

### Common Performance Issues

**Problem**: Hash join shows `Batches: 16` or more.
**Cause**: Inner relation too large for `work_mem`.
**Fix**: Increase `work_mem` for the session, or restructure query to reduce inner size.

**Problem**: NestLoop with high `loops` count and slow inner.
**Cause**: No index on inner relation's join column.
**Fix**: Create index, or let planner choose hash/merge join.

**Problem**: Seq Scan on large table with low selectivity.
**Cause**: Missing index or stale statistics.
**Fix**: `CREATE INDEX` on filter columns; `ANALYZE` the table.

**Problem**: Sort node with `Sort Method: external merge`.
**Cause**: Sort data exceeds `work_mem`; spills to disk.
**Fix**: Increase `work_mem` or add index to avoid sort.

**Problem**: `Rows Removed by Filter` much larger than returned rows.
**Cause**: Qualification pushdown not effective.
**Fix**: Ensure filter predicates are on indexed columns.

### Key GUCs for Executor Tuning

| GUC | Default | Controls |
|-----|---------|----------|
| `work_mem` | 4MB | Memory for sort/hash operations |
| `hash_mem_multiplier` | 2.0 | Multiplier on work_mem for hash operations |
| `max_parallel_workers_per_gather` | 2 | Max workers per parallel scan |
| `max_parallel_workers` | 8 | Total parallel workers system-wide |
| `jit_above_cost` | 100000 | Cost threshold for JIT compilation |
| `enable_hashjoin` | on | Enable/disable hash join |
| `enable_mergejoin` | on | Enable/disable merge join |
| `enable_nestloop` | on | Enable/disable nested loop join |
| `enable_seqscan` | on | Enable/disable sequential scan |
| `effective_io_concurrency` | 1 | Prefetch distance for bitmap scans |

### Executor Hook Points

```
ExecutorStart_hook  --> Before plan state tree is built
ExecutorRun_hook    --> Before tuple retrieval loop
ExecutorFinish_hook --> Before AFTER triggers fire
ExecutorEnd_hook    --> Before resource cleanup
```

Used by: `pg_stat_statements`, `auto_explain`, `sepgsql`, custom extensions.

### SPI Quick Reference

```c
SPI_connect();                          /* Enter SPI */
ret = SPI_execute("SELECT ...", true, 0);  /* Execute SQL */
/* Access results via SPI_tuptable, SPI_processed */
SPI_finish();                           /* Exit SPI */
```

---

## Memory Context Hierarchy

```
TopMemoryContext
  +-- MessageContext (per-command)
        +-- ExecutorState (es_query_cxt)
              +-- ExprContext per-query memory
              +-- ExprContext per-tuple memory  <-- reset every tuple
              +-- Hash table contexts
              +-- Sort contexts
```

**Rule**: Per-tuple results go in `ecxt_per_tuple_memory`. Everything else goes in
`es_query_cxt`. Call `ResetExprContext()` or `ResetPerTupleExprContext()` to free
per-tuple allocations.
