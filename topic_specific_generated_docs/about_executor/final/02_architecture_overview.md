# Chapter 02 -- Architecture Overview

**Prerequisites**: [Chapter 01 -- Executive Summary](01_executive_summary.md)
**Next**: [Chapter 03 -- Executor Lifecycle](03_executor_lifecycle.md)

---

## System Context

The executor is one stage in the PostgreSQL query processing pipeline. It
receives a `PlannedStmt` tree from the planner and produces result tuples that
are sent to a `DestReceiver` (client connection, SPI buffer, tuplestore, etc.).

```
                         PostgreSQL Backend
  +--------+   +--------+   +---------+   +---------+   +-----------+
  | Parser |-->|Analyzer|-->|Rewriter |-->| Planner |-->| EXECUTOR  |
  +--------+   +--------+   +---------+   +---------+   +-----------+
       |                                        |              |
   raw parse     Query         Query tree    PlannedStmt   Result tuples
    tree                                      (Plan tree)   -> DestReceiver
```

The boundary between the planner and executor is the `PlannedStmt` / `Plan`
tree. The boundary between the executor and the rest of the system is the
`QueryDesc` structure, which encapsulates the plan together with snapshots,
parameters, and output destination.

## Structural Diagram

The executor's internal architecture has three major layers:

```
+------------------------------------------------------------------+
|                      ENTRY LAYER (execMain.c)                     |
|  ExecutorStart -> ExecutorRun -> ExecutorFinish -> ExecutorEnd     |
|  QueryDesc, EState, InitPlan, ExecutePlan                         |
+------------------------------------------------------------------+
         |                    |                    |
         v                    v                    v
+------------------+  +------------------+  +------------------+
| DISPATCH LAYER   |  | EXPRESSION LAYER |  | MEMORY LAYER     |
| (execProcnode.c) |  | (execExpr*.c)    |  | (execUtils.c)    |
|                  |  |                  |  |                  |
| ExecInitNode     |  | ExecInitExpr     |  | CreateExecutor   |
| ExecProcNode     |  | ExecInitExprRec  |  |   State          |
| ExecEndNode      |  | ExecInterpExpr   |  | CreateExprContext|
| ExecReScan       |  | ExecQual         |  | ResetExprContext |
| MultiExecProc    |  | ExecProject      |  | FreeExecutor     |
|   Node           |  | JIT compilation  |  |   State          |
+------------------+  +------------------+  +------------------+
         |                                          |
         v                                          v
+------------------------------------------------------------------+
|                      NODE LAYER (node*.c)                         |
|  43 plan node types, each implementing Init/Proc/End              |
|  SeqScan, IndexScan, NestLoop, HashJoin, Sort, Agg, ...          |
+------------------------------------------------------------------+
         |                    |                    |
         v                    v                    v
+------------------+  +------------------+  +------------------+
| TUPLE LAYER      |  | STORAGE LAYER    |  | SUPPORT LAYER    |
| (execTuples.c)   |  | (Table/Index AM) |  | (execScan.c,     |
|                  |  |                  |  |  execAmi.c,       |
| TupleTableSlot   |  | table_beginscan  |  |  execPartition.c)|
| SlotOps dispatch |  | index_beginscan  |  |                  |
| Deforming        |  | heap_getnext     |  | ExecScan loop    |
| Materialization  |  | index_getnext    |  | Partition routing |
+------------------+  +------------------+  +------------------+
```

## Layer Responsibilities

### Entry Layer (`execMain.c`)

The top-level control flow. Manages the four lifecycle phases (Start, Run,
Finish, End), creates and destroys the per-query `EState`, and drives the
main tuple retrieval loop in `ExecutePlan()`. All external callers
(`ProcessQuery`, `PortalRunSelect`, SPI) interact with this layer.

See [Chapter 03 -- Executor Lifecycle](03_executor_lifecycle.md) for full detail.

### Dispatch Layer (`execProcnode.c`, `execAmi.c`)

The Volcano model implementation. `ExecInitNode()` builds the PlanState tree
by dispatching on NodeTag. `ExecProcNode()` retrieves tuples through function
pointer dispatch. `ExecEndNode()` cleans up recursively. `ExecReScan()`
handles parameterized rescanning.

See [Chapter 04 -- Volcano Iterator Model](04_volcano_iterator_model.md).

### Expression Layer (`execExpr.c`, `execExprInterp.c`)

Compiles expression trees into flat step arrays (`ExprState`) and evaluates
them. Provides `ExecQual()` for WHERE/JOIN condition checking and
`ExecProject()` for computing output columns. Supports JIT compilation via
LLVM as an alternative to the step interpreter.

See [Chapter 06 -- Expression Evaluation](06_expression_evaluation.md).

### Memory Layer (`execUtils.c`)

Manages the memory context hierarchy: per-query (`es_query_cxt`), per-node,
and per-tuple (`ecxt_per_tuple_memory`). The `ExprContext` structure connects
expressions to their input tuples and memory management.

See [Chapter 07 -- Memory Context Management](07_memory_context_management.md).

### Node Layer (`node*.c`)

The 43 concrete plan node implementations. Each node type provides three
functions (Init, Proc, End) that plug into the dispatch layer. Nodes are
grouped into categories: scan, join, aggregation, modification, control,
parallel, materialization, and auxiliary.

See Chapters 08-19 for individual node documentation.

### Tuple Layer (`execTuples.c`)

The `TupleTableSlot` abstraction that all tuples flow through. Four concrete
slot types handle different storage formats (virtual, heap, minimal, buffer)
via a virtual method table (`TupleTableSlotOps`).

See [Chapter 05 -- TupleTableSlot Abstraction](05_tuple_table_slot.md).

## Data Flow Overview

A typical SELECT query flows through the executor as follows:

1. **Caller** creates `QueryDesc` with the `PlannedStmt` and calls
   `ExecutorStart()`.
2. **ExecutorStart** creates the `EState`, calls `InitPlan()`, which
   recursively calls `ExecInitNode()` to build the PlanState tree.
3. **Caller** calls `ExecutorRun()`, which enters `ExecutePlan()`.
4. **ExecutePlan** repeatedly calls `ExecProcNode()` on the root PlanState.
5. **Each node** pulls tuples from its children, applies quals via
   `ExecQual()`, and projects results via `ExecProject()`.
6. **Tuples** flow upward through `TupleTableSlot` containers. Each node
   resets its `ExprContext` per tuple to prevent memory leaks.
7. **ExecutePlan** sends each tuple to the `DestReceiver` and stops when
   the count is reached or the plan is exhausted.
8. **Caller** calls `ExecutorFinish()` to fire AFTER triggers.
9. **Caller** calls `ExecutorEnd()` to clean up. `FreeExecutorState()`
   destroys the per-query memory context, freeing everything at once.

## Plan Node Categories

The 43 plan node types fall into 8 categories:

| Category | Count | Examples | Chapter |
|----------|-------|----------|---------|
| Scan | 17 | SeqScan, IndexScan, BitmapHeapScan, SubqueryScan | 08, 15-17 |
| Join | 3 | NestLoop, MergeJoin, HashJoin | 10 |
| Aggregation | 5 | Agg, WindowAgg, Group, Unique, SetOp | 11 |
| Modification | 2 | ModifyTable, LockRows | 12 |
| Control | 6 | Result, Append, MergeAppend, Limit, RecursiveUnion, ProjectSet | 19 |
| Parallel | 2 | Gather, GatherMerge | 13 |
| Materialization | 4 | Sort, Material, Memoize, IncrementalSort | 18 |
| Auxiliary | 4 | Hash, BitmapAnd, BitmapOr, BitmapIndexScan | 10, 16 |

## Extension Points

The executor provides several extension mechanisms:

| Mechanism | Purpose | Used By |
|-----------|---------|---------|
| Executor hooks (`ExecutorStart_hook`, etc.) | Intercept lifecycle phases | pg_stat_statements, auto_explain |
| Custom Scan nodes | Pluggable scan implementations | Foreign data wrappers, custom storage |
| Table AM interface | Pluggable heap storage | Alternative storage engines |
| Index AM interface | Pluggable index implementations | Custom index types |
| JIT provider interface | Pluggable JIT compilation | LLVM JIT provider |
| `ExecutorCheckPerms_hook` | Custom permission checking | sepgsql, row-level security |
