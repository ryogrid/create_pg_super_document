# Executor-Planner Interface

## Overview

The executor consumes the planner's output -- a `PlannedStmt` containing a tree
of `Plan` nodes -- and transforms it into an executable `PlanState` tree. This
document describes the structures and mechanisms through which the planner
communicates its decisions to the executor: the plan tree structure, parameter
passing, subplan/initplan handling, and runtime partition pruning.

Source files:
- `src/include/nodes/plannodes.h` -- PlannedStmt, Plan, and all plan node types
- `src/include/nodes/execnodes.h` -- PlanState and all state node types
- `src/backend/executor/execMain.c` -- InitPlan (plan tree consumption)
- `src/backend/executor/nodeSubplan.c` -- SubPlan/InitPlan execution

## Key Concepts

- **PlannedStmt**: The top-level wrapper containing the plan tree, range table,
  subplans, and execution parameters.
- **Plan to PlanState correspondence**: Each Plan node has a PlanState counterpart.
  The Plan is read-only; the PlanState is mutable runtime state.
- **PARAM_EXTERN vs PARAM_EXEC**: Two parameter mechanisms for passing values
  into and within the plan tree.
- **SubPlan/InitPlan**: Mechanisms for executing subqueries within the plan tree.

## Plan Tree Structure

### PlannedStmt

The planner produces a `PlannedStmt` as its top-level output:

```c
/* Source: src/include/nodes/plannodes.h:46-100 */
typedef struct PlannedStmt
{
    NodeTag     type;
    CmdType     commandType;        /* select|insert|update|delete|merge */
    uint64      queryId;            /* query identifier */
    bool        hasReturning;       /* RETURNING clause present? */
    bool        hasModifyingCTE;    /* modifying CTE in WITH? */
    bool        canSetTag;          /* set command result tag? */
    bool        transientPlan;      /* redo plan on TransactionXmin change */
    bool        dependsOnRole;      /* plan specific to current role? */
    bool        parallelModeNeeded; /* parallel mode required? */
    int         jitFlags;           /* JIT compilation options */

    struct Plan *planTree;          /* root of the Plan node tree */
    List       *rtable;             /* range table (List of RangeTblEntry) */
    List       *permInfos;          /* permission info for rtable entries */
    List       *resultRelations;    /* RT indexes for INSERT/UPDATE/DELETE */
    List       *appendRelations;    /* AppendRelInfo nodes */
    List       *subplans;           /* Plan trees for SubPlan expressions */
    Bitmapset  *rewindPlanIDs;      /* subplan IDs needing REWIND */
    List       *rowMarks;           /* PlanRowMark list */
    List       *relationOids;       /* OIDs of relations used */
    List       *invalItems;         /* other dependency items */
    List       *paramExecTypes;     /* type OIDs for PARAM_EXEC params */
    Node       *utilityStmt;        /* utility statement if CMD_UTILITY */
    ParseLoc    stmt_location;      /* source position */
    ParseLoc    stmt_len;           /* source length */
} PlannedStmt;
```

### Plan (Base Node)

Every plan node type inherits from the `Plan` base structure:

```c
/* Source: src/include/nodes/plannodes.h:119-172 */
typedef struct Plan
{
    NodeTag     type;

    /* Cost estimates */
    Cost        startup_cost;       /* cost before first tuple */
    Cost        total_cost;         /* total cost for all tuples */
    Cardinality plan_rows;          /* estimated row count */
    int         plan_width;         /* average row width in bytes */

    /* Parallel query */
    bool        parallel_aware;     /* participates in parallel execution? */
    bool        parallel_safe;      /* safe to run in parallel worker? */
    bool        async_capable;      /* supports async execution? */

    /* Structure */
    int         plan_node_id;       /* unique across entire plan tree */
    List       *targetlist;         /* target list (output columns) */
    List       *qual;               /* implicitly-ANDed qual conditions */
    struct Plan *lefttree;          /* outer child plan */
    struct Plan *righttree;         /* inner child plan */
    List       *initPlan;           /* InitPlan SubPlan nodes */

    /* Parameter tracking for rescan */
    Bitmapset  *extParam;           /* external PARAM_EXEC IDs */
    Bitmapset  *allParam;           /* all PARAM_EXEC IDs affecting this node */
} Plan;
```

### Plan to PlanState Mapping

During `ExecInitNode()`, each Plan node is transformed into a PlanState node:

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

The Plan tree is read-only -- the executor never modifies it. Multiple
PlanState trees can be built from the same Plan tree (e.g., in parallel query,
each worker builds its own PlanState tree from the serialized Plan).

---

## Target List and Projection

### Plan Target List

The `targetlist` field of a Plan node is a `List` of `TargetEntry` nodes:

```c
typedef struct TargetEntry
{
    Expr        xpr;
    Expr       *expr;           /* expression to compute */
    AttrNumber  resno;          /* column position in result */
    char       *resname;        /* column name (or NULL) */
    Index       ressortgroupref; /* for ORDER BY, GROUP BY */
    Oid         resorigtbl;     /* original table OID */
    AttrNumber  resorigcol;     /* original column number */
    bool        resjunk;        /* junk column (not in output) */
} TargetEntry;
```

### Simple Target List Optimization

The executor detects cases where the target list is a trivial identity mapping
(each entry is a Var with the same attribute number as its position). In such
cases, `ps_ProjInfo` is set to NULL and the scan node returns tuples directly
without projection. This is checked by `tlist_matches_tupdesc()` in
`execUtils.c`.

---

## Parameterized Plans

### Parameter Types

PostgreSQL uses two types of executor parameters:

#### PARAM_EXTERN

External parameters supplied by the caller (e.g., prepared statement bind
values, PL/pgSQL variables):

```c
/* Accessed via econtext->ecxt_param_list_info */
typedef struct ParamListInfoData
{
    /* ... */
    int         numParams;
    ParamExternData params[FLEXIBLE_ARRAY_MEMBER];
} ParamListInfoData;
```

These are set before `ExecutorStart` and remain constant throughout execution.

#### PARAM_EXEC

Internal parameters used for communication between plan nodes within a single
query. These are allocated as an array in the EState:

```c
/* Source: standard_ExecutorStart, execMain.c:185-192 */
if (queryDesc->plannedstmt->paramExecTypes != NIL)
{
    int nParamExec = list_length(queryDesc->plannedstmt->paramExecTypes);
    estate->es_param_exec_vals = (ParamExecData *)
        palloc0(nParamExec * sizeof(ParamExecData));
}
```

PARAM_EXEC parameters are used for:
1. **NestLoop parameter passing**: The outer side of a NestLoop writes values
   that the inner side reads (parameterized index scans).
2. **SubPlan result passing**: InitPlans store their results in PARAM_EXEC slots
   for the main plan to read.
3. **Lateral references**: Cross-references in FROM-clause subqueries.

### NestLoop Parameter Passing

The `NestLoop` plan node has a `nestParams` list of `NestLoopParam` entries.
During execution:

1. Before each inner rescan, `ExecNestLoop` evaluates the parameter expressions
   against the current outer tuple.
2. The results are stored in `es_param_exec_vals` at the appropriate indices.
3. The inner plan's `chgParam` bitmapset is updated with the changed parameter IDs.
4. When `ExecProcNode` is called on the inner plan, it detects `chgParam` and
   calls `ExecReScan()`, which propagates down to any parameterized scan nodes.
5. The parameterized IndexScan re-evaluates its scan keys using the new parameter
   values.

---

## SubPlan and InitPlan

### SubPlan

A `SubPlan` represents a correlated subquery in the plan tree. It appears as
an expression node and is evaluated each time the containing expression is
evaluated:

```c
typedef struct SubPlan
{
    NodeTag     type;
    SubLinkType subLinkType;    /* EXISTS, EXPR, ALL, ANY, etc. */
    Node       *testexpr;       /* comparison expression (if any) */
    List       *paramIds;       /* PARAM_EXEC IDs for output */
    int         plan_id;        /* index into PlannedStmt.subplans */
    /* ... */
} SubPlan;
```

During expression compilation (`ExecInitExprRec`), SubPlan nodes generate
`EEOP_SUBPLAN` steps. At runtime, these steps:
1. Execute the subplan via its PlanState
2. Collect the result (scalar, EXISTS check, or set comparison)
3. Store the result for the outer expression

### InitPlan

An `InitPlan` is an uncorrelated subquery that needs to be executed only once.
Its result is stored in a PARAM_EXEC slot:

1. During `InitPlan()` in `execMain.c`, all entries in `PlannedStmt.subplans`
   are initialized via `ExecInitNode()`.
2. During `ExecInitNode()`, initPlan entries are initialized via
   `ExecInitSubPlan()`.
3. When the initPlan's result is first needed, `ExecReScanSetParamPlan()` runs
   the subplan and stores the result in `es_param_exec_vals`.
4. Subsequent references read directly from the parameter slot.

### SubPlan State in ExecInitNode

```c
/* Source: src/backend/executor/execProcnode.c:396-407 */
/* After the main switch dispatch: */
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

## Runtime Partition Pruning

The planner can defer partition pruning to the executor when pruning depends
on runtime values (e.g., parameter values that are not known at plan time).

### Startup Pruning

During `ExecInitAppend()` or `ExecInitMergeAppend()`, if the plan has
`PartitionPruneInfo`, `ExecInitPartitionPruning()` is called:

1. Evaluates partition pruning constraints using available PARAM_EXTERN values.
2. Produces a bitmapset of partitions that can be skipped.
3. Skipped partitions are not initialized (their `ExecInitNode()` is not called).

### Runtime Pruning

For PARAM_EXEC parameters (e.g., NestLoop params), pruning may need to be
re-evaluated:

1. When `chgParam` indicates relevant parameters have changed, the Append/
   MergeAppend node detects this during rescan.
2. Pruning constraints are re-evaluated with the new parameter values.
3. Child partitions are activated or deactivated accordingly.

This is controlled by the `PartitionPruneState` structure, which tracks
which partitions have been initialized and which are currently active.

---

## eflags Propagation

The `eflags` bitmask controls executor capabilities and is passed down through
`ExecInitNode()`. Upper nodes may modify the flags before passing to children:

| Flag | Value | Meaning | Modifier |
|------|-------|---------|----------|
| `EXEC_FLAG_EXPLAIN_ONLY` | 0x0001 | Don't execute, just build plan | Never removed |
| `EXEC_FLAG_EXPLAIN_GENERIC` | 0x0002 | Tolerate missing params | Never removed |
| `EXEC_FLAG_REWIND` | 0x0004 | Support efficient rescan | Added by NestLoop for inner |
| `EXEC_FLAG_BACKWARD` | 0x0008 | Support backward scan | Added by cursor code; Material shields |
| `EXEC_FLAG_MARK` | 0x0010 | Support mark/restore | Added by MergeJoin for inner |
| `EXEC_FLAG_SKIP_TRIGGERS` | 0x0020 | Don't setup triggers | Set for SELECT without modifying CTE |
| `EXEC_FLAG_WITH_NO_DATA` | 0x0040 | REFRESH WITH NO DATA | Suppress empty matview errors |

Example of eflags modification:
- `ExecInitMergeJoin` adds `EXEC_FLAG_MARK` before calling `ExecInitNode` on
  the inner child, because merge join needs mark/restore on the inner input.
- `ExecInitMaterial` removes `EXEC_FLAG_BACKWARD` and `EXEC_FLAG_MARK` before
  passing to its child, because Material absorbs these requirements.
- Subplan initialization in `InitPlan` strips `EXEC_FLAG_REWIND`,
  `EXEC_FLAG_BACKWARD`, and `EXEC_FLAG_MARK` because subplans never need
  backward scan or mark/restore.

---

## Plan Node ID

Each plan node has a unique `plan_node_id` assigned by the planner. This ID
is used:
- In EXPLAIN output to identify nodes
- In parallel query to match plan nodes across leader and workers
- For instrumentation correlation

The IDs are assigned sequentially across the entire plan tree (including
subplans), ensuring uniqueness.

## Implementation Notes

- The Plan tree is allocated in the planner's memory context and typically
  lives in a cached plan (via the plan cache). It must not be modified by the
  executor.
- `PlannedStmt.subplans` is indexed by SubPlan.plan_id - 1. The macro
  `exec_subplan_get_plan(plannedstmt, subplan)` provides access.
- `extParam` and `allParam` bitmapsets on Plan nodes are computed by the
  planner's `SS_finalize_plan` pass. They determine which parameters affect
  each node, driving the `chgParam` propagation mechanism in the executor.
- The separation between PARAM_EXTERN and PARAM_EXEC is fundamental:
  PARAM_EXTERN values are constant for the query's lifetime, while PARAM_EXEC
  values change during execution (e.g., with each outer tuple in a NestLoop).
