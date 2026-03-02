# Chapter 13: Planner-Executor Interface

> **Prerequisites**: [Chapter 3 -- Executor Lifecycle](03_executor_lifecycle.md), [Chapter 5 -- Volcano Iterator Model](05_volcano_model.md)
> **Next**: [Chapter 14 -- Server Programming Interface (SPI)](14_spi.md)
> **Node catalog details**: [Chapter 19 -- Control and Parallel Nodes](19_control_parallel_nodes.md) (for Append/MergeAppend runtime pruning)

---

## 13.1 Overview

The executor consumes the planner's output -- a `PlannedStmt` containing a tree
of `Plan` nodes -- and transforms it into an executable `PlanState` tree. This
chapter describes the structures and mechanisms through which the planner
communicates its decisions to the executor: the plan tree structure, parameter
passing, SubPlan/InitPlan handling, and runtime partition pruning.

**Relevant source files**:
- `src/include/nodes/plannodes.h` -- PlannedStmt, Plan, and all plan node types
- `src/include/nodes/execnodes.h` -- PlanState and all state node types
- `src/backend/executor/execMain.c` -- InitPlan (plan tree consumption)
- `src/backend/executor/nodeSubplan.c` -- SubPlan/InitPlan execution

**Key symbols covered in this chapter**: `PlannedStmt`, `Plan`, `PlanState`,
`PARAM_EXEC`, `SubPlan`, `InitPlan`, `ExecInitNode`, `ExecReScan`.

---

## 13.2 Plan Tree Structure

### PlannedStmt

The planner produces `PlannedStmt` as its top-level output:

```c
/* src/include/nodes/plannodes.h:46-100 */
typedef struct PlannedStmt
{
    NodeTag     type;
    CmdType     commandType;        /* select|insert|update|delete|merge */
    uint64      queryId;
    bool        hasReturning;
    bool        hasModifyingCTE;
    bool        canSetTag;
    bool        transientPlan;
    bool        dependsOnRole;
    bool        parallelModeNeeded;
    int         jitFlags;

    struct Plan *planTree;          /* root of the Plan node tree */
    List       *rtable;             /* range table (List of RangeTblEntry) */
    List       *permInfos;          /* permission info */
    List       *resultRelations;    /* RT indexes for INSERT/UPDATE/DELETE */
    List       *appendRelations;    /* AppendRelInfo nodes */
    List       *subplans;           /* Plan trees for SubPlan expressions */
    Bitmapset  *rewindPlanIDs;      /* subplan IDs needing REWIND */
    List       *rowMarks;           /* PlanRowMark list */
    List       *relationOids;       /* OIDs of relations used */
    List       *invalItems;         /* other dependency items */
    List       *paramExecTypes;     /* type OIDs for PARAM_EXEC params */
    Node       *utilityStmt;        /* utility statement if CMD_UTILITY */
    ParseLoc    stmt_location;
    ParseLoc    stmt_len;
} PlannedStmt;
```

### Plan (Base Node)

Every plan node type inherits from `Plan`:

```c
/* src/include/nodes/plannodes.h:119-172 */
typedef struct Plan
{
    NodeTag     type;

    /* Cost estimates */
    Cost        startup_cost;
    Cost        total_cost;
    Cardinality plan_rows;
    int         plan_width;

    /* Parallel query */
    bool        parallel_aware;
    bool        parallel_safe;
    bool        async_capable;

    /* Structure */
    int         plan_node_id;       /* unique across entire plan tree */
    List       *targetlist;         /* output columns (List of TargetEntry) */
    List       *qual;               /* implicitly-ANDed qual conditions */
    struct Plan *lefttree;          /* outer child plan */
    struct Plan *righttree;         /* inner child plan */
    List       *initPlan;           /* InitPlan SubPlan nodes */

    /* Parameter tracking for rescan */
    Bitmapset  *extParam;           /* external PARAM_EXEC IDs */
    Bitmapset  *allParam;           /* all PARAM_EXEC IDs affecting this node */
} Plan;
```

---

## 13.3 Plan to PlanState Mapping

During `ExecInitNode()` (see [Chapter 5](05_volcano_model.md)), each Plan node
is transformed into a PlanState node:

| Plan Field | PlanState Field | Transformation |
|------------|----------------|---------------|
| `targetlist` | `ps_ProjInfo` | Compiled via `ExecBuildProjectionInfo()` |
| `qual` | `qual` (ExprState *) | Compiled via `ExecInitQual()` |
| `lefttree` | `lefttree` (PlanState *) | Recursive `ExecInitNode()` |
| `righttree` | `righttree` (PlanState *) | Recursive `ExecInitNode()` |
| `initPlan` | `initPlan` (List of SubPlanState *) | `ExecInitSubPlan()` per entry |
| `plan_node_id` | (via `plan` pointer) | PlanState keeps pointer to original Plan |
| -- | `ExecProcNode` | Set by `ExecSetExecProcNode()` |
| -- | `ps_ExprContext` | Created by `ExecAssignExprContext()` |

The Plan tree is **read-only** -- the executor never modifies it. Multiple
PlanState trees can be built from the same Plan tree (e.g., in parallel query,
each worker builds its own PlanState tree from the serialized Plan; see
[Chapter 12](12_parallel_execution.md)).

---

## 13.4 Target List and Projection

### TargetEntry

The `targetlist` field of a Plan node is a `List` of `TargetEntry` nodes:

```c
typedef struct TargetEntry
{
    Expr        xpr;
    Expr       *expr;           /* expression to compute */
    AttrNumber  resno;          /* column position in result */
    char       *resname;        /* column name (or NULL) */
    Index       ressortgroupref; /* for ORDER BY, GROUP BY */
    Oid         resorigtbl;
    AttrNumber  resorigcol;
    bool        resjunk;        /* junk column (not in final output) */
} TargetEntry;
```

### Simple Target List Optimization

The executor detects cases where the target list is a trivial identity mapping
(each entry is a Var with the same attribute number as its position). In such
cases, `ps_ProjInfo` is set to NULL and the scan node returns tuples directly
without projection. This is checked by `tlist_matches_tupdesc()` in
`execUtils.c`.

This optimization is particularly important for scan nodes (see
[Chapter 8](08_scan_infrastructure.md)), where skipping projection avoids
per-tuple overhead for simple `SELECT *` queries.

---

## 13.5 Parameterized Plans

### Parameter Types

PostgreSQL uses two types of executor parameters:

#### PARAM_EXTERN

External parameters supplied by the caller (prepared statement bind values,
PL/pgSQL variables, SPI parameters):

```c
typedef struct ParamListInfoData
{
    /* ... */
    int         numParams;
    ParamExternData params[FLEXIBLE_ARRAY_MEMBER];
} ParamListInfoData;
```

Set before `ExecutorStart` and constant throughout execution. SPI
([Chapter 14](14_spi.md)) is a major consumer.

#### PARAM_EXEC

Internal parameters for communication between plan nodes within a single query:

```c
/* Allocated in standard_ExecutorStart, execMain.c */
if (queryDesc->plannedstmt->paramExecTypes != NIL)
{
    int nParamExec = list_length(queryDesc->plannedstmt->paramExecTypes);
    estate->es_param_exec_vals = (ParamExecData *)
        palloc0(nParamExec * sizeof(ParamExecData));
}
```

PARAM_EXEC parameters are used for:

1. **NestLoop parameter passing**: The outer side writes values that the inner
   side reads (parameterized index scans; see [Chapter 9](09_join_infrastructure.md))
2. **SubPlan result passing**: InitPlans store results in PARAM_EXEC slots
3. **Lateral references**: Cross-references in FROM-clause subqueries

### NestLoop Parameter Passing

The `NestLoop` plan node has a `nestParams` list of `NestLoopParam` entries.
During execution (detailed in [Chapter 9](09_join_infrastructure.md)):

1. Before each inner rescan, `ExecNestLoop` evaluates parameter expressions
   against the current outer tuple
2. Results are stored in `es_param_exec_vals`
3. The inner plan's `chgParam` bitmapset is updated with changed parameter IDs
4. `ExecProcNode` detects `chgParam` and calls `ExecReScan()`, propagating down
   to parameterized scan nodes
5. The parameterized IndexScan re-evaluates its scan keys using new values
   (see runtime keys in [Chapter 8](08_scan_infrastructure.md))

---

## 13.6 SubPlan and InitPlan

### SubPlan

A `SubPlan` represents a correlated subquery in the plan tree. It appears as
an expression node evaluated each time the containing expression is evaluated:

```c
typedef struct SubPlan
{
    NodeTag     type;
    SubLinkType subLinkType;    /* EXISTS, EXPR, ALL, ANY, etc. */
    Node       *testexpr;       /* comparison expression */
    List       *paramIds;       /* PARAM_EXEC IDs for output */
    int         plan_id;        /* index into PlannedStmt.subplans */
    /* ... */
} SubPlan;
```

During expression compilation (`ExecInitExprRec`, see
[Chapter 7](07_expression_evaluation.md)), SubPlan nodes generate
`EEOP_SUBPLAN` steps. At runtime, these steps:
1. Execute the subplan via its PlanState
2. Collect the result (scalar, EXISTS check, or set comparison)
3. Store the result for the outer expression

### InitPlan

An `InitPlan` is an **uncorrelated** subquery executed only once. Its result is
stored in a PARAM_EXEC slot:

1. During `InitPlan()` in `execMain.c`, all entries in `PlannedStmt.subplans`
   are initialized via `ExecInitNode()`
2. InitPlan entries are initialized via `ExecInitSubPlan()`
3. When the result is first needed, `ExecReScanSetParamPlan()` runs the subplan
   and stores the result in `es_param_exec_vals`
4. Subsequent references read directly from the parameter slot

### SubPlan State Initialization

```c
/* src/backend/executor/execProcnode.c:396-407 */
subps = NIL;
foreach(l, node->initPlan)
{
    SubPlan    *subplan = (SubPlan *) lfirst(l);
    SubPlanState *sstate;

    Assert(IsA(subplan, SubPlan));
    sstate = ExecInitSubPlan(subplan, result);
    subps = lappend(subps, sstate);
}
result->initPlan = subps;
```

---

## 13.7 Runtime Partition Pruning

The planner can defer partition pruning to the executor when pruning depends
on runtime values (parameter values not known at plan time).

### Startup Pruning

During `ExecInitAppend()` or `ExecInitMergeAppend()`, if the plan has
`PartitionPruneInfo`, `ExecInitPartitionPruning()` is called:

1. Evaluates pruning constraints using available PARAM_EXTERN values
2. Produces a bitmapset of partitions that can be skipped
3. Skipped partitions are not initialized (their `ExecInitNode()` is never
   called)

### Runtime Pruning

For PARAM_EXEC parameters (e.g., NestLoop params), pruning may need
re-evaluation:

1. When `chgParam` indicates relevant parameters changed, the Append/
   MergeAppend node detects this during rescan
2. Pruning constraints are re-evaluated with new parameter values
3. Child partitions are activated or deactivated accordingly

This is controlled by `PartitionPruneState`, which tracks which partitions
have been initialized and which are currently active. See
[Chapter 19](19_control_parallel_nodes.md) for Append/MergeAppend node details.

---

## 13.8 eflags Propagation

The `eflags` bitmask controls executor capabilities and is passed down through
`ExecInitNode()`. Upper nodes may modify flags before passing to children:

| Flag | Value | Meaning | Modifier |
|------|-------|---------|----------|
| `EXEC_FLAG_EXPLAIN_ONLY` | 0x0001 | Do not execute, just build plan | Never removed |
| `EXEC_FLAG_EXPLAIN_GENERIC` | 0x0002 | Tolerate missing params | Never removed |
| `EXEC_FLAG_REWIND` | 0x0004 | Support efficient rescan | Added by NestLoop for inner |
| `EXEC_FLAG_BACKWARD` | 0x0008 | Support backward scan | Added by cursor code |
| `EXEC_FLAG_MARK` | 0x0010 | Support mark/restore | Added by MergeJoin for inner |
| `EXEC_FLAG_SKIP_TRIGGERS` | 0x0020 | Do not setup triggers | Set for SELECT |
| `EXEC_FLAG_WITH_NO_DATA` | 0x0040 | REFRESH WITH NO DATA | Suppress matview errors |

Examples of eflags modification:

- `ExecInitMergeJoin` adds `EXEC_FLAG_MARK` for the inner child, because
  merge join needs mark/restore (see [Chapter 9](09_join_infrastructure.md))
- `ExecInitMaterial` removes `EXEC_FLAG_BACKWARD` and `EXEC_FLAG_MARK` for
  its child, because Material absorbs these requirements
- `ExecInitNestLoop` adds `EXEC_FLAG_REWIND` when no `nestParams` exist
- Subplan initialization strips `EXEC_FLAG_REWIND`, `EXEC_FLAG_BACKWARD`,
  and `EXEC_FLAG_MARK`

---

## 13.9 Plan Node ID

Each plan node has a unique `plan_node_id` assigned by the planner. This ID
is used:
- In EXPLAIN output to identify nodes
- In parallel query to match plan nodes across leader and workers
  (see [Chapter 12](12_parallel_execution.md))
- For instrumentation correlation

IDs are assigned sequentially across the entire plan tree (including subplans).

---

## 13.10 Implementation Notes

1. The Plan tree is allocated in the planner's memory context and typically
   lives in a cached plan (via the plan cache). It must not be modified by
   the executor.

2. `PlannedStmt.subplans` is indexed by `SubPlan.plan_id - 1`. The macro
   `exec_subplan_get_plan(plannedstmt, subplan)` provides access.

3. `extParam` and `allParam` bitmapsets on Plan nodes are computed by the
   planner's `SS_finalize_plan` pass. They determine which parameters affect
   each node, driving the `chgParam` propagation mechanism in the executor.

4. The separation between PARAM_EXTERN and PARAM_EXEC is fundamental:
   PARAM_EXTERN values are constant for the query's lifetime, while
   PARAM_EXEC values change during execution (e.g., with each outer tuple
   in a NestLoop).

---

**See also**: [Chapter 3 -- Executor Lifecycle](03_executor_lifecycle.md) for
how InitPlan builds the PlanState tree, [Chapter 5](05_volcano_model.md) for
ExecInitNode dispatch, [Chapter 9](09_join_infrastructure.md) for NestLoop
parameter mechanics, [Chapter 14](14_spi.md) for how SPI provides PARAM_EXTERN
values.
