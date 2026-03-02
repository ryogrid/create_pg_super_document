# Executor API Reference

**PostgreSQL 17.6 -- Function Signatures by Subsystem**

All signatures verified against `src/include/executor/*.h` and implementation files.

---

## Table of Contents

1. [Lifecycle Functions](#1-lifecycle-functions)
2. [Node Dispatch Functions](#2-node-dispatch-functions)
3. [Expression Evaluation Functions](#3-expression-evaluation-functions)
4. [Tuple Slot Functions](#4-tuple-slot-functions)
5. [Memory Management Functions](#5-memory-management-functions)
6. [SPI Functions](#6-spi-functions)
7. [Scan Infrastructure](#7-scan-infrastructure)
8. [Utility Functions](#8-utility-functions)

---

## 1. Lifecycle Functions

**Source**: `src/include/executor/executor.h`, `src/backend/executor/execMain.c`

### ExecutorStart

```c
void ExecutorStart(QueryDesc *queryDesc, int eflags);
```

Initialize executor for a query plan. Creates `EState`, builds `PlanState` tree.
Must be called before `ExecutorRun`. Invokes `ExecutorStart_hook` if set.

**Parameters**:
- `queryDesc`: Query descriptor with plan and snapshots
- `eflags`: Bitmask of `EXEC_FLAG_*` (EXPLAIN_ONLY, BACKWARD, REWIND, etc.)

### standard_ExecutorStart

```c
void standard_ExecutorStart(QueryDesc *queryDesc, int eflags);
```

Standard implementation. Called by `ExecutorStart` if no hook is installed.

### ExecutorRun

```c
void ExecutorRun(QueryDesc *queryDesc,
                 ScanDirection direction, uint64 count,
                 bool execute_once);
```

Drive tuple retrieval. Calls `ExecutePlan()` internally. May be called multiple
times for cursor-based execution.

**Parameters**:
- `queryDesc`: Initialized query descriptor (estate must be set)
- `direction`: `ForwardScanDirection`, `BackwardScanDirection`, or `NoMovementScanDirection`
- `count`: Maximum tuples to return; 0 = no limit
- `execute_once`: Reserved for future use

### standard_ExecutorRun

```c
void standard_ExecutorRun(QueryDesc *queryDesc,
                          ScanDirection direction, uint64 count,
                          bool execute_once);
```

### ExecutorFinish

```c
void ExecutorFinish(QueryDesc *queryDesc);
```

Post-execution cleanup. Fires AFTER triggers via `AfterTriggerEndQuery()`.
Must be called after the last `ExecutorRun` and before `ExecutorEnd`.

### standard_ExecutorFinish

```c
void standard_ExecutorFinish(QueryDesc *queryDesc);
```

### ExecutorEnd

```c
void ExecutorEnd(QueryDesc *queryDesc);
```

Final cleanup. Calls `ExecEndNode()` recursively, releases snapshots,
destroys per-query memory context via `FreeExecutorState()`.

### standard_ExecutorEnd

```c
void standard_ExecutorEnd(QueryDesc *queryDesc);
```

### ExecutorRewind

```c
void ExecutorRewind(QueryDesc *queryDesc);
```

Rewind executor to the beginning for re-execution. Calls `ExecReScan()` on
the root plan node.

### CreateQueryDesc

```c
QueryDesc *CreateQueryDesc(PlannedStmt *plannedstmt,
                           const char *sourceText,
                           Snapshot snapshot,
                           Snapshot crosscheck_snapshot,
                           DestReceiver *dest,
                           ParamListInfo params,
                           QueryEnvironment *queryEnv,
                           int instrument_options);
```

Construct a `QueryDesc` from planner output and execution parameters.

---

## 2. Node Dispatch Functions

**Source**: `src/include/executor/executor.h`, `src/backend/executor/execProcnode.c`

### ExecInitNode

```c
PlanState *ExecInitNode(Plan *node, EState *estate, int eflags);
```

Recursively initialize all nodes in a plan tree. Dispatches on `NodeTag` to
call node-specific init functions (e.g., `ExecInitSeqScan`, `ExecInitHashJoin`).
Returns the root of the `PlanState` tree.

### ExecProcNode

```c
/* Inline function in src/include/executor/executor.h */
static inline TupleTableSlot *
ExecProcNode(PlanState *node);
```

Pull next tuple from a plan node. Calls through `node->ExecProcNode` function
pointer. Returns NULL when the node is exhausted.

### ExecEndNode

```c
void ExecEndNode(PlanState *node);
```

Recursively clean up all nodes in the plan tree. Dispatches on `NodeTag`.

### ExecReScan

```c
void ExecReScan(PlanState *node);
```

Reset a plan node for rescanning. Checks `chgParam` to determine if parameters
changed. Dispatches to node-specific rescan functions.

### MultiExecProcNode

```c
Node *MultiExecProcNode(PlanState *node);
```

Execute plan nodes that return non-tuple results (hash tables, bitmaps).
Used by `T_Hash`, `T_BitmapIndexScan`, `T_BitmapAnd`, `T_BitmapOr`.

### ExecSetExecProcNode

```c
void ExecSetExecProcNode(PlanState *node, ExecProcNodeMtd function);
```

Set or change the execution function for a plan node. Installs wrapper
functions for instrumentation and stack depth checking.

### ExecShutdownNode

```c
void ExecShutdownNode(PlanState *node);
```

Controlled shutdown: stop parallel workers, release async resources.

### ExecSetTupleBound

```c
void ExecSetTupleBound(int64 tuples_needed, PlanState *child_node);
```

Propagate tuple count limits for top-N sort optimization.

---

## 3. Expression Evaluation Functions

**Source**: `src/include/executor/executor.h`, `src/backend/executor/execExpr.c`

### ExecInitExpr

```c
ExprState *ExecInitExpr(Expr *node, PlanState *parent);
```

Compile an expression tree into an `ExprState` with a flat step array.
Returns NULL if `node` is NULL.

### ExecInitExprWithParams

```c
ExprState *ExecInitExprWithParams(Expr *node, ParamListInfo ext_params);
```

Compile expression with external parameters but no parent PlanState.

### ExecInitQual

```c
ExprState *ExecInitQual(List *qual, PlanState *parent);
```

Compile a list of qualification expressions (implicitly ANDed) with
short-circuit `EEOP_QUAL` steps.

### ExecInitExprList

```c
List *ExecInitExprList(List *nodes, PlanState *parent);
```

Compile a list of expressions, returning a `List` of `ExprState *`.

### ExecEvalExpr

```c
/* Inline function */
static inline Datum
ExecEvalExpr(ExprState *state,
             ExprContext *econtext,
             bool *isNull);
```

Evaluate a compiled expression. Dispatches through `state->evalfunc`.

### ExecEvalExprSwitchContext

```c
/* Inline function */
static inline Datum
ExecEvalExprSwitchContext(ExprState *state,
                          ExprContext *econtext,
                          bool *isNull);
```

Same as `ExecEvalExpr` but switches to `ecxt_per_tuple_memory` first.

### ExecQual

```c
/* Inline function */
static inline bool
ExecQual(ExprState *state, ExprContext *econtext);
```

Evaluate a qualification expression (boolean). NULL state returns true.
Uses `ExecEvalExprSwitchContext` internally.

### ExecProject

```c
/* Inline function */
static inline TupleTableSlot *
ExecProject(ProjectionInfo *projInfo);
```

Evaluate target list into result slot. Returns the filled slot.

### ExecBuildProjectionInfo

```c
ProjectionInfo *ExecBuildProjectionInfo(List *targetList,
                                        ExprContext *econtext,
                                        TupleTableSlot *slot,
                                        PlanState *parent,
                                        TupleDesc inputDesc);
```

Build a `ProjectionInfo` from a target list. Detects identity projections.

---

## 4. Tuple Slot Functions

**Source**: `src/include/executor/tuptable.h`

### Slot Creation

```c
TupleTableSlot *MakeTupleTableSlot(TupleDesc tupleDesc,
                                    const TupleTableSlotOps *tts_ops);
```

Create a standalone slot with the given descriptor and operations.

```c
TupleTableSlot *ExecAllocTableSlot(List **tupleTable, TupleDesc desc,
                                    const TupleTableSlotOps *tts_ops);
```

Allocate a slot and register it in the EState's tuple table list.

### Tuple Storage

```c
TupleTableSlot *ExecStoreHeapTuple(HeapTuple tuple,
                                    TupleTableSlot *slot,
                                    bool shouldFree);
```

Store a `HeapTuple` in a slot. If `shouldFree`, the tuple is freed on clear.

```c
TupleTableSlot *ExecStoreBufferHeapTuple(HeapTuple tuple,
                                          TupleTableSlot *slot,
                                          Buffer buffer);
```

Store a tuple from a shared buffer. The slot holds a pin on the buffer.

```c
TupleTableSlot *ExecStorePinnedBufferHeapTuple(HeapTuple tuple,
                                                TupleTableSlot *slot,
                                                Buffer buffer);
```

Like `ExecStoreBufferHeapTuple` but the caller already holds the pin.

```c
TupleTableSlot *ExecStoreMinimalTuple(MinimalTuple mtup,
                                       TupleTableSlot *slot,
                                       bool shouldFree);
```

Store a `MinimalTuple` (used by hash tables, tuplestores).

```c
TupleTableSlot *ExecStoreVirtualTuple(TupleTableSlot *slot);
```

Mark a slot as containing a valid virtual tuple (Datum/isnull arrays already filled).

```c
TupleTableSlot *ExecStoreAllNullTuple(TupleTableSlot *slot);
```

Fill slot with all-NULL values. Used for outer join null-extension.

### Slot Operations

```c
/* Inline function */
static inline TupleTableSlot *
ExecClearTuple(TupleTableSlot *slot);
```

Clear the slot, releasing any resources (buffer pin, pfree). Sets `TTS_FLAG_EMPTY`.

```c
void ExecStoreHeapTupleDatum(Datum data, TupleTableSlot *slot);
```

Store a composite-type Datum as a tuple in the slot.

### Predefined Slot Operations

| Global | Type |
|--------|------|
| `TTSOpsVirtual` | Virtual (Datum arrays) |
| `TTSOpsHeapTuple` | HeapTuple (not in buffer) |
| `TTSOpsMinimalTuple` | MinimalTuple |
| `TTSOpsBufferHeapTuple` | HeapTuple in shared buffer (holds pin) |

---

## 5. Memory Management Functions

**Source**: `src/include/executor/executor.h`

### EState Management

```c
EState *CreateExecutorState(void);
```

Create a new `EState` with its own `es_query_cxt` memory context.

```c
void FreeExecutorState(EState *estate);
```

Destroy the EState and its entire memory context.

### ExprContext Management

```c
ExprContext *CreateExprContext(EState *estate);
```

Create an `ExprContext` linked to the given EState. Allocates per-query and
per-tuple memory contexts.

```c
ExprContext *CreateStandaloneExprContext(void);
```

Create an ExprContext not linked to any EState (for standalone use).

```c
ExprContext *MakePerTupleExprContext(EState *estate);
```

Create or return the per-tuple ExprContext for the EState.

```c
/* Macro */
#define ResetExprContext(econtext) \
    MemoryContextReset((econtext)->ecxt_per_tuple_memory)
```

Reset the per-tuple memory context, freeing all per-tuple allocations.

```c
/* Macro */
#define ResetPerTupleExprContext(estate) \
    do { \
        if ((estate)->es_per_tuple_exprcontext) \
            ResetExprContext((estate)->es_per_tuple_exprcontext); \
    } while (0)
```

Reset the EState's per-tuple expression context if it exists.

---

## 6. SPI Functions

**Source**: `src/include/executor/spi.h`

### Connection Management

```c
int SPI_connect(void);
```

Establish an SPI connection. Must be called before any SPI operations.
Returns `SPI_OK_CONNECT` on success.

```c
int SPI_connect_ext(int options);
```

Extended version with options (e.g., `SPI_OPT_NONATOMIC`).

```c
int SPI_finish(void);
```

Close the SPI connection and clean up.

### Query Execution

```c
int SPI_execute(const char *src, bool read_only, long tcount);
```

Parse, plan, and execute a SQL string.

**Parameters**:
- `src`: SQL query string
- `read_only`: true if the query does not modify data
- `tcount`: Maximum rows to return; 0 = no limit

**Returns**: `SPI_OK_SELECT`, `SPI_OK_INSERT`, etc. Results available via
`SPI_tuptable` and `SPI_processed`.

### Prepared Statements

```c
SPIPlanPtr SPI_prepare(const char *src, int nargs, Oid *argtypes);
```

Prepare a parameterized SQL statement. Returns a plan handle for later execution.

**Parameters**:
- `src`: SQL with `$1`, `$2`, ... parameter placeholders
- `nargs`: Number of parameters
- `argtypes`: Array of parameter type OIDs

---

## 7. Scan Infrastructure

**Source**: `src/include/executor/executor.h`, `src/backend/executor/execScan.c`

### ExecScan

```c
TupleTableSlot *ExecScan(ScanState *node,
                          ExecScanAccessMtd accessMtd,
                          ExecScanRecheckMtd recheckMtd);
```

Generic scan loop used by all scan nodes. Implements the pattern:
1. Call `accessMtd` to fetch next tuple
2. Call `ExecQual` to test qualification
3. Call `ExecProject` to compute output
4. If qual fails, loop back to step 1

**Parameters**:
- `node`: Scan state
- `accessMtd`: Function returning the next raw tuple
- `recheckMtd`: Function to recheck tuple against original quals (for lossy scans)

### ExecScanFetch (internal)

```c
static TupleTableSlot *
ExecScanFetch(ScanState *node,
              ExecScanAccessMtd accessMtd,
              ExecScanRecheckMtd recheckMtd);
```

Internal helper that handles EvalPlanQual substitution during concurrent updates.

---

## 8. Utility Functions

**Source**: `src/include/executor/executor.h`

### Permission Checking

```c
bool ExecCheckPermissions(List *rangeTable, List *rteperminfos, bool abort);
```

Verify ACL permissions for all range table entries. If `abort` is true,
raises an error on failure.

### Row Mark Support

```c
ExecRowMark *ExecFindRowMark(EState *estate, Index rti, bool missing_ok);
```

Find the `ExecRowMark` for a given range table index (for SELECT FOR UPDATE).

### EvalPlanQual

```c
TupleTableSlot *EvalPlanQual(EPQState *epqstate, Relation relation,
                              Index rti, TupleTableSlot *inputslot);
```

Recheck a concurrently modified tuple under READ COMMITTED.

### Junk Filter

```c
JunkFilter *ExecInitJunkFilter(List *targetList, TupleTableSlot *slot);
```

Create a junk filter to strip resjunk attributes from output tuples.

```c
TupleTableSlot *ExecFilterJunk(JunkFilter *junkfilter, TupleTableSlot *slot);
```

Apply the junk filter to a tuple, returning the cleaned result.

### Result Relation

```c
void InitResultRelInfo(ResultRelInfo *resultRelInfo,
                       Relation resultRelationDesc,
                       Index resultRelationIndex,
                       ResultRelInfo *partition_root_rri,
                       int instrument_options);
```

Initialize a `ResultRelInfo` for a DML target relation.

### Constraint Checking

```c
void ExecConstraints(ResultRelInfo *resultRelInfo,
                     TupleTableSlot *slot, EState *estate);
```

Evaluate CHECK constraints and NOT NULL constraints on a tuple.

### Mark/Restore

```c
void ExecMarkPos(PlanState *node);
void ExecRestrPos(PlanState *node);
bool ExecSupportsMarkRestore(struct Path *pathnode);
```

Save and restore scan position. Used by MergeJoin.

### Backward Scan

```c
bool ExecSupportsBackwardScan(Plan *node);
```

Check if a plan node supports backward scanning (for cursors).
