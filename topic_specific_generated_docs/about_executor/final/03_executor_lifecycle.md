# Chapter 03 -- Executor Lifecycle

**Prerequisites**: [Chapter 02 -- Architecture Overview](02_architecture_overview.md)
**Next**: [Chapter 04 -- Volcano Iterator Model](04_volcano_iterator_model.md)

**Key symbols**: `ExecutorStart`, `standard_ExecutorStart`, `ExecutorRun`,
`standard_ExecutorRun`, `ExecutorFinish`, `ExecutorEnd`, `QueryDesc`, `EState`,
`InitPlan`, `ExecutePlan`, `CreateQueryDesc`, `PortalRunSelect`

---

## Overview

The executor lifecycle is managed through four top-level entry points defined
in `src/backend/executor/execMain.c`:

1. `ExecutorStart()` -- build PlanState tree, allocate resources
2. `ExecutorRun()` -- retrieve tuples (may be called multiple times)
3. `ExecutorFinish()` -- fire AFTER triggers, process modifying CTEs
4. `ExecutorEnd()` -- release all resources

These four functions form a strict sequential protocol. Every query execution
must call them in this order. Each provides a hook mechanism for loadable
plugins: if the corresponding `*_hook` function pointer is non-NULL, the hook
is called instead of the standard implementation. Hooks typically call the
`standard_*` function internally after performing their own work.

```
                 Lifecycle State Diagram

  [QueryDesc created]
         |
         v
  ExecutorStart(queryDesc, eflags)
         |
         v
  ExecutorRun(queryDesc, direction, count) <--+
         |                                     |
         | (may be called multiple times       |
         |  for cursor FETCH)                  |
         +-------------------------------------+
         |
         v
  ExecutorFinish(queryDesc)
         |
         v
  ExecutorEnd(queryDesc)
         |
         v
  [QueryDesc fields cleared]
```

For the full state diagram with internal substeps, see
`diagrams/executor_lifecycle.mermaid`.

---

## QueryDesc -- The Bridge Structure

`QueryDesc` encapsulates everything the executor needs to process a query.
Created by `CreateQueryDesc()` before the executor is invoked, it carries
the plan from the planner and accumulates executor state during initialization.

```c
/* Source: src/include/executor/execdesc.h */
typedef struct QueryDesc
{
    CmdType     operation;          /* CMD_SELECT, CMD_UPDATE, etc. */
    PlannedStmt *plannedstmt;       /* planner's output (plan tree) */
    const char *sourceText;         /* source text of the query */
    Snapshot    snapshot;            /* MVCC snapshot for visibility */
    Snapshot    crosscheck_snapshot; /* crosscheck for RI update/delete */
    DestReceiver *dest;             /* destination for tuple output */
    ParamListInfo params;           /* parameter values passed in */
    QueryEnvironment *queryEnv;     /* query environment */
    int         instrument_options; /* OR of InstrumentOption flags */

    /* Fields set by ExecutorStart */
    TupleDesc   tupDesc;            /* descriptor for result tuples */
    EState     *estate;             /* executor's per-query state */
    PlanState  *planstate;          /* root of PlanState tree */

    /* Fields set by ExecutePlan */
    bool        already_executed;   /* true if previously executed */

    /* Optional instrumentation (set by EXPLAIN ANALYZE) */
    struct Instrumentation *totaltime;
} QueryDesc;
```

| Field | Set By | Purpose |
|-------|--------|---------|
| `plannedstmt` | Caller | Complete planner output with plan tree, range table, subplans |
| `snapshot` | Caller | MVCC snapshot determining tuple visibility |
| `dest` | Caller | Output destination (client, SPI buffer, tuplestore, EXPLAIN) |
| `estate` | `ExecutorStart` | Per-query execution state (see [EState](#estate----per-query-execution-state)) |
| `planstate` | `ExecutorStart` | Root of the runtime PlanState tree (see [Chapter 04](04_volcano_iterator_model.md)) |
| `totaltime` | EXPLAIN ANALYZE | Captures overall `ExecutorRun` duration |

---

## ExecutorStart

### Purpose

Must be called at the beginning of execution of any query plan. Builds the
PlanState tree, opens relations, registers snapshots, and prepares all
resources for execution.

### Signature

```c
/* Source: src/backend/executor/execMain.c:120 */
void ExecutorStart(QueryDesc *queryDesc, int eflags);
```

### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `queryDesc` | `QueryDesc *` | Query descriptor with plan and snapshots; `estate` must not be set yet |
| `eflags` | `int` | Bitmask of `EXEC_FLAG_*` constants controlling capabilities |

### eflags Constants

| Flag | Purpose |
|------|---------|
| `EXEC_FLAG_EXPLAIN_ONLY` | Do not actually execute; used by EXPLAIN without ANALYZE |
| `EXEC_FLAG_BACKWARD` | Plan must support backward scan (scrollable cursors) |
| `EXEC_FLAG_REWIND` | Plan must support rewinding to start |
| `EXEC_FLAG_MARK` | Plan must support mark/restore for merge joins |
| `EXEC_FLAG_SKIP_TRIGGERS` | Skip trigger execution (internal use) |

### Hook Mechanism

```c
void ExecutorStart(QueryDesc *queryDesc, int eflags)
{
    pgstat_report_query_id(queryDesc->plannedstmt->queryId, false);

    if (ExecutorStart_hook)
        (*ExecutorStart_hook) (queryDesc, eflags);
    else
        standard_ExecutorStart(queryDesc, eflags);
}
```

### standard_ExecutorStart -- Step by Step

```c
/* Source: src/backend/executor/execMain.c:140 */
void standard_ExecutorStart(QueryDesc *queryDesc, int eflags)
```

1. **Sanity checks**: Assert `queryDesc` is not NULL and `estate` is not
   already set. Verify the active snapshot matches `queryDesc->snapshot`.

2. **Read-only check**: If the transaction is read-only or in parallel mode
   and not EXPLAIN-only, call `ExecCheckXactReadOnly()` to reject writes to
   non-temporary tables.

3. **Create EState**: `CreateExecutorState()` allocates a per-query memory
   context named "ExecutorState" and creates the `EState` within it. See
   [Chapter 07](07_memory_context_management.md) for details.

4. **Switch to per-query context**: `MemoryContextSwitchTo(estate->es_query_cxt)`
   ensures all subsequent allocations live for the query's lifetime.

5. **Setup parameters**: Copy external parameters (`es_param_list_info`) and
   allocate workspace for internal `PARAM_EXEC` values (`es_param_exec_vals`).

6. **Set command ID**: For INSERT/UPDATE/DELETE/MERGE and SELECT FOR UPDATE,
   call `GetCurrentCommandId(true)` to obtain the CID for marking output tuples.

7. **Register snapshots**: `RegisterSnapshot()` for both the main and
   crosscheck snapshots to prevent premature release.

8. **Copy flags**: Set `es_top_eflags`, `es_instrument`, `es_jit_flags`.

9. **Trigger context**: Unless `EXEC_FLAG_SKIP_TRIGGERS` or
   `EXEC_FLAG_EXPLAIN_ONLY`, call `AfterTriggerBeginQuery()`.

10. **Initialize plan tree**: Call `InitPlan(queryDesc, eflags)`.

11. **Restore memory context**: Switch back to the caller's context.

### Callers

- `PortalRunSelect()` -- cursor/portal bridge (see [Portal Integration](#portal-integration))
- `ProcessQuery()` -- simple query protocol path (`src/backend/tcop/pquery.c`)
- `SPI_execute_plan_extended()` -- internal SQL from PL/pgSQL
- `refresh_matview_datafill()` -- materialized view refresh

---

## InitPlan

### Purpose

Central initialization routine called by `standard_ExecutorStart`. Checks
permissions, sets up the range table, initializes subplans, and recursively
constructs the PlanState tree via `ExecInitNode()`.

### Signature

```c
/* Source: src/backend/executor/execMain.c:825 */
static void InitPlan(QueryDesc *queryDesc, int eflags);
```

### Step-by-Step Logic

1. **Permission checks**: `ExecCheckPermissions()` verifies ACL permissions on
   all relations in the range table. The `ExecutorCheckPerms_hook` allows
   extensions like sepgsql to add custom checks.

2. **Range table setup**: `ExecInitRangeTable()` copies the range table list
   into the EState and allocates the `es_relations` array.

3. **Row marks**: For SELECT FOR UPDATE/SHARE, build the `ExecRowMark` array
   from `PlanRowMark` entries, opening relations as needed.

4. **Initialize tuple table**: Set `es_tupleTable = NIL`.

5. **Initialize subplans**: For each entry in `plannedstmt->subplans`, call
   `ExecInitNode()` to build the subplan's PlanState tree. Subplans never
   need BACKWARD or MARK capabilities but may need REWIND.

6. **Build main PlanState tree**: `ExecInitNode(plan, estate, eflags)`
   recursively constructs the PlanState tree. This is the most critical step --
   it invokes all node-type-specific initialization routines. See
   [Chapter 04](04_volcano_iterator_model.md) for `ExecInitNode` details.

7. **Result tuple descriptor**: `ExecGetResultType(planstate)` extracts the
   output tuple descriptor.

8. **Junk filter**: For SELECT queries, if any target list entries have
   `resjunk` set, create a `JunkFilter` to strip them before output.

9. **Store results**: Set `queryDesc->tupDesc` and `queryDesc->planstate`.

---

## ExecutorRun

### Purpose

Main routine of the executor module. Drives tuple retrieval by calling
`ExecutePlan()`, which repeatedly invokes `ExecProcNode()` on the root plan
node. May be called multiple times for cursor-based execution.

### Signature

```c
/* Source: src/backend/executor/execMain.c:299 */
void ExecutorRun(QueryDesc *queryDesc,
                 ScanDirection direction, uint64 count,
                 bool execute_once);
```

### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `queryDesc` | `QueryDesc *` | Initialized query descriptor (`estate` must be set) |
| `direction` | `ScanDirection` | `ForwardScanDirection`, `BackwardScanDirection`, or `NoMovementScanDirection` |
| `count` | `uint64` | Maximum tuples to retrieve; 0 means no limit |
| `execute_once` | `bool` | Currently ignored; exists for API stability |

### standard_ExecutorRun -- Step by Step

```c
/* Source: src/backend/executor/execMain.c:310 */
void standard_ExecutorRun(QueryDesc *queryDesc,
                          ScanDirection direction, uint64 count,
                          bool execute_once)
```

1. Switch to per-query memory context.
2. If `totaltime` instrumentation is active, call `InstrStartNode()`.
3. Determine whether tuples need to be sent (`CMD_SELECT` or `hasReturning`).
4. Start the destination receiver: `dest->rStartup(dest, operation, tupDesc)`.
5. If direction is not `NoMovementScanDirection`, call `ExecutePlan()`.
6. Update `es_total_processed` counter.
7. Shut down destination receiver: `dest->rShutdown(dest)`.
8. Stop instrumentation timer.
9. Restore caller's memory context.

---

## ExecutePlan -- The Inner Loop

### Purpose

The core execution loop that repeatedly calls `ExecProcNode()` on the root
plan node until the requested number of tuples has been retrieved or the plan
is exhausted.

### Signature

```c
/* Source: src/backend/executor/execMain.c:1596 */
static void ExecutePlan(QueryDesc *queryDesc,
                        CmdType operation, bool sendTuples,
                        uint64 numberTuples, ScanDirection direction,
                        DestReceiver *dest);
```

### Main Loop

```c
for (;;)
{
    ResetPerTupleExprContext(estate);     /* free per-tuple memory */
    slot = ExecProcNode(planstate);       /* get next tuple */
    if (TupIsNull(slot)) break;          /* no more tuples */
    if (junkFilter) slot = ExecFilterJunk(junkFilter, slot);
    if (sendTuples && !dest->receiveSlot(slot, dest)) break;
    if (operation == CMD_SELECT) (estate->es_processed)++;
    current_tuple_count++;
    if (numberTuples && numberTuples == current_tuple_count) break;
}
```

Key aspects of this loop:

- **`ResetPerTupleExprContext()`** is called every iteration to free memory
  from the previous tuple's expression evaluation. This is critical for
  preventing memory leaks in long-running queries. See
  [Chapter 07](07_memory_context_management.md) for the memory context
  hierarchy.

- **`ExecProcNode()`** is the Volcano pull interface. See
  [Chapter 04](04_volcano_iterator_model.md) for how this dispatches to
  node-specific functions.

- **Junk filter**: Applied only when junk attributes exist in the top-level
  target list (e.g., `ctid` for UPDATE/DELETE).

- **Destination receiver**: The `dest->receiveSlot()` callback sends tuples
  to the client, SPI buffer, or other destination. It can return false to
  request early termination.

After the loop, if backward scan is not needed, `ExecShutdownNode()` is
called to release parallel workers and other resources early.

---

## ExecutorFinish

### Purpose

Must be called after the last `ExecutorRun` call and before `ExecutorEnd`.
Handles post-execution tasks.

### Signature

```c
/* Source: src/backend/executor/execMain.c:399 */
void ExecutorFinish(QueryDesc *queryDesc);
```

### standard_ExecutorFinish

1. Assert that `es_finished` is false (prevent double-finish).
2. Switch to per-query memory context.
3. Start instrumentation timing if active.
4. Call `ExecPostprocessPlan()`: runs any secondary ModifyTable nodes (from
   modifying CTEs) to completion, ensuring all side effects occur even if the
   main query did not consume all CTE output.
5. Call `AfterTriggerEndQuery()`: execute all queued AFTER triggers.
6. Stop instrumentation timing.
7. Set `estate->es_finished = true`.

`ExecutorFinish` was introduced in PostgreSQL 9.1 to separate AFTER trigger
processing from resource cleanup, enabling EXPLAIN ANALYZE to include trigger
time in its measurement.

---

## ExecutorEnd

### Purpose

Final cleanup. Must be called at the end of execution. Releases all resources
including the per-query memory context.

### Signature

```c
/* Source: src/backend/executor/execMain.c:459 */
void ExecutorEnd(QueryDesc *queryDesc);
```

### standard_ExecutorEnd

1. Assert `es_finished` is true (or EXPLAIN-only mode).
2. Switch to per-query memory context.
3. Call `ExecEndPlan()`:
   - `ExecEndNode(planstate)` -- recursively cleans up all plan nodes.
     See [Chapter 04](04_volcano_iterator_model.md#execendnode).
   - `ExecEndNode()` per subplan state.
   - `ExecResetTupleTable()` -- releases buffer pins and tuple descriptor
     reference counts for all registered `TupleTableSlot` instances.
   - `ExecCloseResultRelations()` -- closes indexes and ancestor relations.
   - `ExecCloseRangeTableRelations()` -- closes all opened range table
     relations.
4. Unregister snapshots: `UnregisterSnapshot()` for both snapshots.
5. Switch back to caller's memory context.
6. Call `FreeExecutorState(estate)` -- destroys the per-query memory context,
   which frees all executor-allocated memory including the EState itself.
   See [Chapter 07](07_memory_context_management.md#freeexecutorstate).
7. Reset `queryDesc` fields (`estate`, `planstate`, `tupDesc`) to NULL.

---

## EState -- Per-Query Execution State

The EState is the shared execution state for all plan nodes in a single
executor invocation. It is created by `CreateExecutorState()` and destroyed
by `FreeExecutorState()`.

```c
/* Source: src/include/nodes/execnodes.h (selected fields) */
typedef struct EState
{
    NodeTag     type;
    ScanDirection es_direction;
    Snapshot    es_snapshot;
    Snapshot    es_crosscheck_snapshot;
    List       *es_range_table;
    Index       es_range_table_size;
    Relation   *es_relations;
    PlannedStmt *es_plannedstmt;
    CommandId   es_output_cid;
    ResultRelInfo **es_result_relations;
    ParamListInfo es_param_list_info;
    ParamExecData *es_param_exec_vals;
    MemoryContext es_query_cxt;         /* per-query memory context */
    List       *es_tupleTable;
    uint64      es_processed;
    uint64      es_total_processed;
    int         es_top_eflags;
    int         es_instrument;
    bool        es_finished;
    List       *es_exprcontexts;
    List       *es_subplanstates;
    List       *es_auxmodifytables;
    ExprContext *es_per_tuple_exprcontext;
    int         es_jit_flags;
    struct JitContext *es_jit;
} EState;
```

| Field | Purpose | Lifecycle |
|-------|---------|-----------|
| `es_query_cxt` | Root memory context for all executor allocations | Created in `CreateExecutorState`, destroyed in `FreeExecutorState` |
| `es_snapshot` | Registered snapshot for MVCC visibility | Registered in `standard_ExecutorStart`, unregistered in `standard_ExecutorEnd` |
| `es_param_exec_vals` | Array for passing values between plan nodes (NestLoop params, SubPlan results) | Allocated in `standard_ExecutorStart` |
| `es_subplanstates` | PlanState nodes for all subplans referenced by SubPlan expressions | Built during `InitPlan` |
| `es_per_tuple_exprcontext` | Shared ExprContext for per-output-tuple operations (constraints, index insertion) | Created on demand, reset per tuple in `ExecutePlan` |
| `es_exprcontexts` | List of all ExprContexts for cleanup ordering | Maintained by `CreateExprContext`, consumed by `FreeExecutorState` |

For memory management details, see [Chapter 07](07_memory_context_management.md).

---

## Portal Integration

The portal subsystem (`src/backend/tcop/pquery.c`) bridges between SQL-level
cursors and the executor.

`PortalRunSelect()` calls `ExecutorRun()` with the appropriate direction and
count for FETCH/MOVE commands:

- **Forward scan**: `ForwardScanDirection` -- the default for normal queries.
- **Backward scan**: `BackwardScanDirection` -- requires `EXEC_FLAG_BACKWARD`
  to have been passed to `ExecutorStart`. Only scrollable cursors support this.
- **No movement**: `NoMovementScanDirection` -- used for MOVE 0.

For held (materialized) cursors, the portal stores results in a tuplestore
rather than calling the executor for each FETCH. This allows cursor results
to survive transaction boundaries.

---

## EXPLAIN ANALYZE Instrumentation

When `EXPLAIN ANALYZE` is used, the executor collects timing and buffer usage
statistics for each plan node.

### Setup

1. `queryDesc->instrument_options` is set with `INSTRUMENT_TIMER`,
   `INSTRUMENT_BUFFERS`, and/or `INSTRUMENT_WAL` flags.
2. `standard_ExecutorStart` copies these to `estate->es_instrument`.
3. In `ExecInitNode()`, if `es_instrument` is set, each node gets an
   `Instrumentation` structure via `InstrAlloc()`.
4. `ExecSetExecProcNode()` installs `ExecProcNodeFirst` as the initial
   wrapper.

### Runtime Instrumentation

On the first call to `ExecProcNode()`, `ExecProcNodeFirst()` checks whether
instrumentation is present and installs the appropriate wrapper:

```c
/* Source: src/backend/executor/execProcnode.c:442 */
static TupleTableSlot *
ExecProcNodeFirst(PlanState *node)
{
    check_stack_depth();
    if (node->instrument)
        node->ExecProcNode = ExecProcNodeInstr;
    else
        node->ExecProcNode = node->ExecProcNodeReal;
    return node->ExecProcNode(node);
}
```

The instrumented wrapper brackets each call with timing:

```c
/* Source: src/backend/executor/execProcnode.c:473 */
static TupleTableSlot *
ExecProcNodeInstr(PlanState *node)
{
    TupleTableSlot *result;
    InstrStartNode(node->instrument);
    result = node->ExecProcNodeReal(node);
    InstrStopNode(node->instrument, TupIsNull(result) ? 0.0 : 1.0);
    return result;
}
```

`queryDesc->totaltime` captures the overall `ExecutorRun` duration. This is
set by the EXPLAIN ANALYZE code before calling `ExecutorRun` and read after
completion. For detailed instrumentation internals, see Chapter 20.

---

## Hook Summary

| Hook Variable | Called In | Typical Users |
|--------------|-----------|---------------|
| `ExecutorStart_hook` | `ExecutorStart()` | pg_stat_statements, auto_explain |
| `ExecutorRun_hook` | `ExecutorRun()` | auto_explain |
| `ExecutorFinish_hook` | `ExecutorFinish()` | pg_stat_statements |
| `ExecutorEnd_hook` | `ExecutorEnd()` | pg_stat_statements |
| `ExecutorCheckPerms_hook` | `ExecCheckPermissions()` | sepgsql, row-level security |

All hook type definitions are in `src/include/executor/executor.h`.
