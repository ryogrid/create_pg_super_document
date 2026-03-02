# Executor Lifecycle

## Overview

The PostgreSQL executor lifecycle is managed through four top-level entry points --
`ExecutorStart`, `ExecutorRun`, `ExecutorFinish`, and `ExecutorEnd` -- defined in
`src/backend/executor/execMain.c`. These functions form a strict sequential protocol
that every query execution must follow. Each entry point provides a hook mechanism
for loadable plugins (such as `pg_stat_statements` or `auto_explain`) while
delegating actual work to `standard_*` implementations.

The executor sits at the end of the query processing pipeline:

    parse -> analyze -> rewrite -> plan -> **execute**

Callers such as `ProcessQuery()` (in `src/backend/tcop/pquery.c`) and
`PortalRunSelect()` bridge between the portal/cursor infrastructure and the
executor. The `QueryDesc` structure serves as the primary interface object,
encapsulating everything the executor needs to run a query.

## Key Concepts

- **QueryDesc**: The bridge structure carrying plan, snapshots, parameters, and
  destination receiver from the traffic cop to the executor.
- **EState**: Per-query execution state, including memory contexts, range table,
  result relations, and parameter storage.
- **Hook mechanism**: Each lifecycle function checks a global function pointer
  (e.g., `ExecutorStart_hook`); if non-NULL, the hook is called instead of
  the standard implementation. Hooks typically call `standard_*` internally.
- **eflags**: Bitmask controlling execution capabilities (`EXEC_FLAG_EXPLAIN_ONLY`,
  `EXEC_FLAG_BACKWARD`, `EXEC_FLAG_REWIND`, `EXEC_FLAG_MARK`).

## Architecture

```
See: diagrams/executor_lifecycle.mermaid
```

## Core APIs

### QueryDesc

#### Purpose

Encapsulates all information the executor needs to process a query. Created by
`CreateQueryDesc()` before the executor is invoked, and populated further by
`ExecutorStart`.

#### Definition

```c
/* Source: src/include/executor/execdesc.h:33-56 */
typedef struct QueryDesc
{
    CmdType     operation;          /* CMD_SELECT, CMD_UPDATE, etc. */
    PlannedStmt *plannedstmt;       /* planner's output */
    const char *sourceText;         /* source text of the query */
    Snapshot    snapshot;            /* snapshot to use for query */
    Snapshot    crosscheck_snapshot; /* crosscheck for RI update/delete */
    DestReceiver *dest;             /* destination for tuple output */
    ParamListInfo params;           /* param values being passed in */
    QueryEnvironment *queryEnv;     /* query environment passed in */
    int         instrument_options; /* OR of InstrumentOption flags */

    /* Set by ExecutorStart */
    TupleDesc   tupDesc;            /* descriptor for result tuples */
    EState     *estate;             /* executor's query-wide state */
    PlanState  *planstate;          /* tree of per-plan-node state */

    /* Set by ExecutePlan */
    bool        already_executed;   /* true if previously executed */

    /* Set to NULL by core; plugins may change */
    struct Instrumentation *totaltime; /* total ExecutorRun time */
} QueryDesc;
```

#### Key Fields

| Field | Type | Description |
|-------|------|-------------|
| `operation` | `CmdType` | The command type: CMD_SELECT, CMD_INSERT, CMD_UPDATE, CMD_DELETE, CMD_MERGE |
| `plannedstmt` | `PlannedStmt *` | The complete planner output including plan tree, range table, and subplans |
| `snapshot` | `Snapshot` | MVCC snapshot for tuple visibility |
| `dest` | `DestReceiver *` | Output destination (client, tuplestore, SPI, etc.) |
| `estate` | `EState *` | Executor state, set during ExecutorStart |
| `planstate` | `PlanState *` | Root of the runtime plan state tree, set during ExecutorStart |
| `totaltime` | `Instrumentation *` | Set by EXPLAIN ANALYZE to capture total runtime |

---

### ExecutorStart

#### Purpose

Must be called at the beginning of execution of any query plan. Initializes the
executor state, builds the plan state tree, and prepares all resources for
execution.

#### Signature

```c
/* Source: src/backend/executor/execMain.c:120-137 */
void ExecutorStart(QueryDesc *queryDesc, int eflags);
```

#### Parameters

| Parameter | Type | Description | Constraints |
|-----------|------|-------------|-------------|
| `queryDesc` | `QueryDesc *` | Query descriptor with plan and snapshots | Must not have `estate` already set |
| `eflags` | `int` | Bitmask of EXEC_FLAG_* constants | See executor.h |

#### Detailed Description

1. Reports query_id to statistics subsystem via `pgstat_report_query_id()`.
2. Checks `ExecutorStart_hook` -- if set, calls the hook; otherwise calls
   `standard_ExecutorStart()`.

#### Step-by-Step Logic of standard_ExecutorStart

```c
/* Source: src/backend/executor/execMain.c:139-263 */
void standard_ExecutorStart(QueryDesc *queryDesc, int eflags)
```

1. **Sanity checks**: Assert `queryDesc` is not NULL and `estate` is not already set.
   Verify active snapshot matches `queryDesc->snapshot`.

2. **Read-only check**: If the transaction is read-only or in parallel mode, and
   not EXPLAIN-only, call `ExecCheckXactReadOnly()` to reject writes to
   non-temporary tables.

3. **Create EState**: `CreateExecutorState()` allocates a per-query memory context
   named "ExecutorState" and creates the `EState` node within it.

4. **Switch to per-query context**: `MemoryContextSwitchTo(estate->es_query_cxt)`
   ensures all subsequent allocations live for the query's lifetime.

5. **Setup parameters**: Copy external parameters (`es_param_list_info`) and
   allocate workspace for internal PARAM_EXEC values (`es_param_exec_vals`).

6. **Set command ID**: For INSERT/UPDATE/DELETE/MERGE and SELECT FOR UPDATE,
   call `GetCurrentCommandId(true)` to obtain the CID for marking output tuples.

7. **Register snapshots**: `RegisterSnapshot()` for both the main snapshot and
   crosscheck snapshot to prevent premature release.

8. **Copy state**: Set `es_top_eflags`, `es_instrument`, `es_jit_flags`.

9. **Trigger context**: Unless `EXEC_FLAG_SKIP_TRIGGERS` or `EXEC_FLAG_EXPLAIN_ONLY`,
   call `AfterTriggerBeginQuery()`.

10. **Initialize plan tree**: Call `InitPlan(queryDesc, eflags)`.

11. **Restore memory context**: Switch back to the caller's context.

#### Integration Points

- **Called by**: `PortalRunSelect()`, `ProcessQuery()`, `SPI_execute_plan_extended()`,
  `refresh_matview_datafill()`
- **Calls**: `CreateExecutorState()`, `InitPlan()`, `AfterTriggerBeginQuery()`,
  `RegisterSnapshot()`

---

### InitPlan

#### Purpose

Central initialization routine that builds the entire executor state: checks
permissions, sets up the range table, initializes subplans, and recursively
constructs the plan state tree via `ExecInitNode`.

#### Signature

```c
/* Source: src/backend/executor/execMain.c:825-1003 */
static void InitPlan(QueryDesc *queryDesc, int eflags);
```

#### Step-by-Step Logic

1. **Permission checks**: `ExecCheckPermissions()` verifies that the current user
   has appropriate ACL permissions on all relations in the range table.

2. **Range table setup**: `ExecInitRangeTable()` copies the range table list into
   the EState and allocates the `es_relations` array.

3. **Row marks**: For SELECT FOR UPDATE/SHARE, build the `ExecRowMark` array from
   `PlanRowMark` entries, opening relations as needed.

4. **Initialize tuple table**: Set `es_tupleTable = NIL`.

5. **Initialize subplans**: For each entry in `plannedstmt->subplans`, call
   `ExecInitNode()` to build the subplan's state tree. Subplans never need
   BACKWARD or MARK capabilities but may need REWIND.

6. **Build main plan state tree**: `ExecInitNode(plan, estate, eflags)` recursively
   constructs the PlanState tree from the Plan tree. This is the most critical
   step -- it invokes all node-type-specific initialization routines.

7. **Result tuple descriptor**: `ExecGetResultType(planstate)` extracts the output
   tuple descriptor.

8. **Junk filter**: For SELECT queries, if any target list entries have `resjunk`
   set, create a `JunkFilter` to strip them before output.

9. **Store results**: Set `queryDesc->tupDesc` and `queryDesc->planstate`.

#### Integration Points

- **Called by**: `standard_ExecutorStart()`
- **Calls**: `ExecCheckPermissions()`, `ExecInitRangeTable()`, `ExecInitNode()`,
  `ExecInitSubPlan()`, `ExecInitJunkFilter()`

---

### ExecutorRun

#### Purpose

Main routine of the executor module. Drives tuple retrieval by calling
`ExecutePlan()`, which repeatedly invokes `ExecProcNode()` on the root plan node.
May be called multiple times for cursor-based execution.

#### Signature

```c
/* Source: src/backend/executor/execMain.c:298-307 */
void ExecutorRun(QueryDesc *queryDesc,
                 ScanDirection direction, uint64 count,
                 bool execute_once);
```

#### Parameters

| Parameter | Type | Description | Constraints |
|-----------|------|-------------|-------------|
| `queryDesc` | `QueryDesc *` | Initialized query descriptor | `estate` must be set |
| `direction` | `ScanDirection` | Forward, backward, or no movement | Backward requires EXEC_FLAG_BACKWARD |
| `count` | `uint64` | Max tuples to retrieve; 0 = no limit | Non-negative |
| `execute_once` | `bool` | Ignored in current code (API compatibility) | -- |

#### Step-by-Step Logic of standard_ExecutorRun

```c
/* Source: src/backend/executor/execMain.c:309-383 */
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

### ExecutePlan

#### Purpose

The inner execution loop that repeatedly calls `ExecProcNode()` on the root
plan node until the requested number of tuples has been retrieved or the plan
is exhausted.

#### Signature

```c
/* Source: src/backend/executor/execMain.c:1596-1710 */
static void ExecutePlan(QueryDesc *queryDesc,
                        CmdType operation, bool sendTuples,
                        uint64 numberTuples, ScanDirection direction,
                        DestReceiver *dest);
```

#### Step-by-Step Logic

1. **Parallel mode**: If query has not been partially executed and has no tuple
   count limit, enter parallel mode if `parallelModeNeeded` is set.

2. **Main loop**:
   ```c
   for (;;)
   {
       ResetPerTupleExprContext(estate);     /* free per-tuple memory */
       slot = ExecProcNode(planstate);        /* get next tuple */
       if (TupIsNull(slot)) break;            /* no more tuples */
       if (junkFilter) slot = ExecFilterJunk(junkFilter, slot);
       if (sendTuples && !dest->receiveSlot(slot, dest)) break;
       if (operation == CMD_SELECT) (estate->es_processed)++;
       current_tuple_count++;
       if (numberTuples && numberTuples == current_tuple_count) break;
   }
   ```

3. **Shutdown**: If backward scan is not needed, call `ExecShutdownNode()` to
   release parallel workers and other resources.

4. Exit parallel mode if entered.

#### Performance Considerations

- `ResetPerTupleExprContext()` is called every iteration to prevent memory leaks
  from expression evaluation. This resets the per-tuple memory context, which is
  critical for long-running queries.
- The junk filter is only applied when junk attributes exist in the top-level
  target list (e.g., `ctid` for UPDATE/DELETE).

---

### ExecutorFinish

#### Purpose

Must be called after the last `ExecutorRun` call and before `ExecutorEnd`. It
handles post-execution tasks such as firing AFTER triggers.

#### Signature

```c
/* Source: src/backend/executor/execMain.c:399-406 */
void ExecutorFinish(QueryDesc *queryDesc);
```

#### Step-by-Step Logic of standard_ExecutorFinish

```c
/* Source: src/backend/executor/execMain.c:408-445 */
void standard_ExecutorFinish(QueryDesc *queryDesc)
```

1. Assert that `es_finished` is false (not called twice).
2. Switch to per-query memory context.
3. Start instrumentation timing if active.
4. Call `ExecPostprocessPlan()`: runs any secondary ModifyTable nodes
   (from modifying CTEs) to completion.
5. Call `AfterTriggerEndQuery()`: execute all queued AFTER triggers.
6. Stop instrumentation timing.
7. Set `estate->es_finished = true`.

---

### ExecutorEnd

#### Purpose

Final cleanup. Must be called at the end of execution. Releases all resources
including the per-query memory context.

#### Signature

```c
/* Source: src/backend/executor/execMain.c:459-516 */
void ExecutorEnd(QueryDesc *queryDesc);
```

#### Step-by-Step Logic of standard_ExecutorEnd

1. Assert `es_finished` is true (or EXPLAIN-only mode).
2. Switch to per-query memory context.
3. Call `ExecEndPlan()`:
   - `ExecEndNode(planstate)` -- recursively cleans up all plan nodes
   - `ExecEndNode()` per subplan state
   - `ExecResetTupleTable()` -- releases buffer pins and tupdesc refcounts
   - `ExecCloseResultRelations()` -- closes indexes and ancestor relations
   - `ExecCloseRangeTableRelations()` -- closes all opened range table relations
4. Unregister snapshots: `UnregisterSnapshot()` for both snapshots.
5. Switch back to caller's memory context.
6. Call `FreeExecutorState(estate)` -- destroys the per-query memory context,
   which frees all executor-allocated memory including the EState itself.
7. Reset `queryDesc` fields to NULL.

---

### EState

#### Purpose

Per-query execution state shared by all plan nodes in a single executor
invocation. Contains snapshots, the range table, result relations, memory
contexts, and parameter storage.

#### Definition

```c
/* Source: src/include/nodes/execnodes.h:621-701 (partial) */
typedef struct EState
{
    NodeTag     type;

    ScanDirection es_direction;     /* current scan direction */
    Snapshot    es_snapshot;         /* MVCC snapshot for tuple visibility */
    Snapshot    es_crosscheck_snapshot; /* RI crosscheck snapshot */
    List       *es_range_table;     /* List of RangeTblEntry */
    Index       es_range_table_size; /* size of the range table arrays */
    Relation   *es_relations;       /* per-RTE Relation pointers */
    PlannedStmt *es_plannedstmt;    /* link to top of plan tree */

    CommandId   es_output_cid;      /* CID for INSERT/UPDATE/DELETE */
    ResultRelInfo **es_result_relations; /* per-RTE ResultRelInfo */

    ParamListInfo es_param_list_info;  /* external params */
    ParamExecData *es_param_exec_vals; /* internal PARAM_EXEC params */

    MemoryContext es_query_cxt;     /* per-query context */
    List       *es_tupleTable;      /* List of TupleTableSlots */

    uint64      es_processed;       /* tuples processed this run */
    uint64      es_total_processed; /* total across all runs */

    int         es_top_eflags;      /* eflags from ExecutorStart */
    int         es_instrument;      /* InstrumentOption flags */
    bool        es_finished;        /* set by ExecutorFinish */

    List       *es_exprcontexts;    /* List of ExprContexts */
    List       *es_subplanstates;   /* List of subplan PlanStates */
    List       *es_auxmodifytables; /* secondary ModifyTableStates */

    ExprContext *es_per_tuple_exprcontext; /* for constraint checks */

    int         es_jit_flags;       /* JIT compilation options */
    struct JitContext *es_jit;      /* JIT context */
} EState;
```

#### Key Fields

| Field | Purpose | Lifecycle |
|-------|---------|-----------|
| `es_query_cxt` | Root memory context for all executor allocations | Created in `CreateExecutorState`, destroyed in `FreeExecutorState` |
| `es_snapshot` | Registered snapshot for MVCC visibility | Registered in `standard_ExecutorStart`, unregistered in `standard_ExecutorEnd` |
| `es_param_exec_vals` | Array for passing values between plan nodes (e.g., NestLoop params, SubPlan results) | Allocated in `standard_ExecutorStart` |
| `es_subplanstates` | PlanState nodes for all subplans referenced by SubPlan expressions | Built during `InitPlan` |
| `es_per_tuple_exprcontext` | Shared ExprContext for per-output-tuple operations (constraints, index) | Created on demand, reset per tuple in `ExecutePlan` |

---

## EXPLAIN ANALYZE Instrumentation

When `EXPLAIN ANALYZE` is used, the executor collects timing and buffer usage
statistics for each plan node.

### Instrumentation Setup

1. `queryDesc->instrument_options` is set with `INSTRUMENT_TIMER`,
   `INSTRUMENT_BUFFERS`, and/or `INSTRUMENT_WAL` flags.
2. `standard_ExecutorStart` copies this to `estate->es_instrument`.
3. In `ExecInitNode()`, if `es_instrument` is set, each node gets an
   `Instrumentation` structure via `InstrAlloc(1, es_instrument, async_capable)`.
4. `ExecSetExecProcNode()` installs `ExecProcNodeFirst` as the initial wrapper.

### Instrumentation Wrappers

On the first call to `ExecProcNode()`, `ExecProcNodeFirst()` checks whether
instrumentation is present:

```c
/* Source: src/backend/executor/execProcnode.c:442-465 */
static TupleTableSlot *
ExecProcNodeFirst(PlanState *node)
{
    check_stack_depth();
    if (node->instrument)
        node->ExecProcNode = ExecProcNodeInstr;  /* wrap with timing */
    else
        node->ExecProcNode = node->ExecProcNodeReal;  /* direct call */
    return node->ExecProcNode(node);
}
```

The instrumented wrapper brackets each call with timing:

```c
/* Source: src/backend/executor/execProcnode.c:473-485 */
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

### Executor-Level Timing

`queryDesc->totaltime` captures the overall ExecutorRun duration. This is set
by the EXPLAIN ANALYZE code before calling ExecutorRun and read after completion.

---

## Portal Integration

The portal subsystem (`src/backend/tcop/pquery.c`) bridges between SQL-level
cursors and the executor:

- `PortalRunSelect()` calls `ExecutorRun()` with the appropriate direction
  and count for FETCH/MOVE commands.
- Forward scan: `ForwardScanDirection`
- Backward scan: `BackwardScanDirection` (requires `EXEC_FLAG_BACKWARD`)
- For held (materialized) cursors, the portal stores results in a tuplestore
  rather than calling the executor for each FETCH.

## Hook Summary

| Hook Variable | Type | Called In | Typical Users |
|--------------|------|-----------|---------------|
| `ExecutorStart_hook` | `ExecutorStart_hook_type` | `ExecutorStart()` | pg_stat_statements, auto_explain |
| `ExecutorRun_hook` | `ExecutorRun_hook_type` | `ExecutorRun()` | auto_explain |
| `ExecutorFinish_hook` | `ExecutorFinish_hook_type` | `ExecutorFinish()` | pg_stat_statements |
| `ExecutorEnd_hook` | `ExecutorEnd_hook_type` | `ExecutorEnd()` | pg_stat_statements |
| `ExecutorCheckPerms_hook` | `ExecutorCheckPerms_hook_type` | `ExecCheckPermissions()` | sepgsql, row-level security |

All hook type definitions are in `src/include/executor/executor.h:74-97`.

## Implementation Notes

- `ExecutorFinish` was introduced in PostgreSQL 9.1 to separate AFTER trigger
  processing from resource cleanup, enabling EXPLAIN ANALYZE to include trigger
  time in its measurement.
- The `execute_once` parameter of `ExecutorRun` is currently ignored and exists
  only for API stability in stable branches.
- `ExecPostprocessPlan()` in `ExecutorFinish` runs secondary ModifyTable nodes
  (from modifying CTEs) to completion. This ensures predictable behavior when
  the main query does not consume all CTE output.
- The per-query memory context created by `CreateExecutorState()` becomes a child
  of the caller's `CurrentMemoryContext` at the time `ExecutorStart` is called.
  This is typically the portal's memory context.
