# Volcano / Iterator Execution Model

## Overview

PostgreSQL implements the Volcano (also called "iterator") execution model, in
which every plan node exposes the same tuple-at-a-time interface. A parent node
pulls tuples from its children by calling `ExecProcNode()`, which returns one
`TupleTableSlot` per call (or NULL to signal end-of-scan). This demand-driven,
pull-based approach provides elegant composability: nodes can be freely combined
into arbitrarily deep trees without changing any individual node's implementation.

The central dispatch functions live in `src/backend/executor/execProcnode.c`
(982 lines). This file contains `ExecInitNode`, `ExecEndNode`,
`MultiExecProcNode`, `ExecShutdownNode`, and `ExecSetTupleBound`. The inline
`ExecProcNode` function itself is defined in `src/include/executor/executor.h`.

## Key Concepts

- **Pull-based execution**: The root node drives execution by requesting tuples
  from children, who in turn request from their children, recursively.
- **Plan tree to PlanState tree**: During initialization, each Plan node is
  transformed into a corresponding PlanState node. The PlanState tree mirrors
  the Plan tree structure but contains runtime state.
- **Function pointer dispatch**: `ExecProcNode()` dispatches through a function
  pointer stored directly in the PlanState node, avoiding per-tuple switch
  overhead.
- **NodeTag dispatch**: `ExecInitNode` and `ExecEndNode` use switch statements
  on the NodeTag to dispatch to type-specific routines.

## Architecture

```
See: diagrams/volcano_tuple_flow.mermaid
See: diagrams/node_dispatch_flowchart.mermaid
```

## Core APIs

### ExecInitNode

#### Purpose

Recursively initializes all nodes in a plan tree, dispatching on NodeTag to call
the appropriate `ExecInit*` function for each node type. Produces a PlanState
tree that mirrors the Plan tree.

#### Signature

```c
/* Source: src/backend/executor/execProcnode.c:141-415 */
PlanState *
ExecInitNode(Plan *node, EState *estate, int eflags);
```

#### Parameters

| Parameter | Type | Description | Constraints |
|-----------|------|-------------|-------------|
| `node` | `Plan *` | Current plan node to initialize | May be NULL (leaf) |
| `estate` | `EState *` | Shared per-query execution state | Must be valid |
| `eflags` | `int` | Capability flags (REWIND, BACKWARD, MARK, etc.) | Modified as passed down |

#### Return Value

Returns a `PlanState *` corresponding to the given Plan node, or NULL if `node`
is NULL.

#### Step-by-Step Logic

1. **NULL check**: If `node` is NULL, return NULL (end of leaf).

2. **Stack depth check**: `check_stack_depth()` prevents stack overflow on deeply
   nested plan trees.

3. **NodeTag dispatch**: A large switch statement (lines 161-389) dispatches to
   the appropriate ExecInit function based on the plan node's type:
   ```c
   switch (nodeTag(node))
   {
       case T_Result:
           result = (PlanState *) ExecInitResult((Result *) node, estate, eflags);
           break;
       case T_SeqScan:
           result = (PlanState *) ExecInitSeqScan((SeqScan *) node, estate, eflags);
           break;
       /* ... 41 more node types ... */
       case T_Limit:
           result = (PlanState *) ExecInitLimit((Limit *) node, estate, eflags);
           break;
   }
   ```

4. **Install execution wrapper**: `ExecSetExecProcNode(result, result->ExecProcNode)`
   wraps the execution function with `ExecProcNodeFirst` for stack checking and
   optional instrumentation.

5. **Initialize initPlans**: For each SubPlan in `node->initPlan`, call
   `ExecInitSubPlan()` to build the SubPlanState.

6. **Allocate instrumentation**: If `estate->es_instrument` is set, allocate an
   `Instrumentation` structure via `InstrAlloc()`.

#### Integration Points

- **Called by**: `InitPlan()` for the root plan and subplans,
  recursively by every `ExecInit*` function for child nodes
- **Calls**: All 43 `ExecInit*` node functions, `ExecSetExecProcNode()`,
  `ExecInitSubPlan()`, `InstrAlloc()`

---

### ExecProcNode

#### Purpose

Central function that processes a single plan node and returns the next tuple.
This is the heart of the Volcano iterator model -- it dispatches to the
node-specific execution function through a function pointer.

#### Signature

```c
/* Source: src/include/executor/executor.h (inline) */
#ifndef FRONTEND
static inline TupleTableSlot *
ExecProcNode(PlanState *node)
{
    if (node->chgParam != NULL) /* something changed? */
        ExecReScan(node);       /* let ReScan handle this */
    return node->ExecProcNode(node);
}
#endif
```

#### Detailed Description

This inline function first checks whether any parameters used by this node have
changed (indicated by `chgParam` being non-NULL). If so, it calls `ExecReScan()`
to reset the node before fetching the next tuple. Then it dispatches through the
`ExecProcNode` function pointer stored in the PlanState.

The function pointer goes through the following states during execution:

1. **After ExecInitNode**: Set to `ExecProcNodeFirst` by `ExecSetExecProcNode()`.
2. **On first call**: `ExecProcNodeFirst()` checks stack depth, then either:
   - If instrumented: sets pointer to `ExecProcNodeInstr`
   - Otherwise: sets pointer to `ExecProcNodeReal` (the actual node function)
3. **Subsequent calls**: Dispatches directly to the appropriate function.

This design avoids both the overhead of a switch statement on every tuple and
the overhead of stack depth checking on every tuple (only done once).

#### Return Value

Returns a `TupleTableSlot *` containing the next tuple, or a slot with
`TTS_FLAG_EMPTY` set (checked via `TupIsNull()`) to signal end-of-scan.

#### Performance Considerations

- The inline definition avoids function call overhead for the most performance-
  critical path in the executor.
- The `chgParam` check is done inline so that the common case (no parameter
  changes) adds only a pointer comparison.
- After the first call, the function pointer is resolved to either the direct
  node function or the instrumented wrapper, eliminating further dispatch overhead.

---

### ExecSetExecProcNode

#### Purpose

Installs or changes the execution function for a plan node, wrapping it with
`ExecProcNodeFirst` for stack depth checking and potential instrumentation.

#### Signature

```c
/* Source: src/backend/executor/execProcnode.c:424-435 */
void
ExecSetExecProcNode(PlanState *node, ExecProcNodeMtd function)
{
    node->ExecProcNodeReal = function;
    node->ExecProcNode = ExecProcNodeFirst;
}
```

#### Detailed Description

The actual execution function is stored in `ExecProcNodeReal`. The visible
`ExecProcNode` pointer is set to `ExecProcNodeFirst`, which performs one-time
setup (stack check, instrumentation wrapper installation) on the first call.

This function is called automatically at the end of `ExecInitNode()`. It may
also be called later by nodes that need to change their execution function
after initialization (e.g., after a parameter change).

---

### ExecProcNodeFirst / ExecProcNodeInstr

#### Purpose

Wrapper functions for one-time initialization and instrumentation.

#### Implementation

```c
/* Source: src/backend/executor/execProcnode.c:442-485 */
static TupleTableSlot *
ExecProcNodeFirst(PlanState *node)
{
    check_stack_depth();

    /* After first call, remove this wrapper */
    if (node->instrument)
        node->ExecProcNode = ExecProcNodeInstr;
    else
        node->ExecProcNode = node->ExecProcNodeReal;

    return node->ExecProcNode(node);
}

static TupleTableSlot *
ExecProcNodeInstr(PlanState *node)
{
    TupleTableSlot *result;

    InstrStartNode(node->instrument);           /* start timing */
    result = node->ExecProcNodeReal(node);      /* execute node */
    InstrStopNode(node->instrument,
                  TupIsNull(result) ? 0.0 : 1.0); /* stop timing */

    return result;
}
```

---

### MultiExecProcNode

#### Purpose

Executes plan nodes that return complex data structures (hash tables, bitmaps)
rather than individual tuples. Only four node types support this interface.

#### Signature

```c
/* Source: src/backend/executor/execProcnode.c:501-542 */
Node *
MultiExecProcNode(PlanState *node);
```

#### Detailed Description

Unlike `ExecProcNode`, this uses a switch statement because only four node types
support it:

| NodeTag | Function | Returns |
|---------|----------|---------|
| `T_HashState` | `MultiExecHash()` | HashJoinTable |
| `T_BitmapIndexScanState` | `MultiExecBitmapIndexScan()` | TIDBitmap |
| `T_BitmapAndState` | `MultiExecBitmapAnd()` | TIDBitmap |
| `T_BitmapOrState` | `MultiExecBitmapOr()` | TIDBitmap |

Before dispatching, if `chgParam` is set, it calls `ExecReScan()` to handle
parameter changes. MultiExecProcNode does not use InstrStartNode/InstrStopNode
because the nodes return complex structures making tuple counting ambiguous.
Each node provides its own instrumentation.

---

### ExecEndNode

#### Purpose

Recursively cleans up all nodes in the plan tree. After this operation, the
plan cannot be processed further.

#### Signature

```c
/* Source: src/backend/executor/execProcnode.c:556-758 */
void
ExecEndNode(PlanState *node);
```

#### Step-by-Step Logic

1. If `node` is NULL, return (leaf node).
2. Check stack depth.
3. Free `chgParam` bitmapset if present.
4. Switch on NodeTag to call the appropriate `ExecEnd*` function.

Three node types have no cleanup action and simply break:
`T_ValuesScanState`, `T_NamedTuplestoreScanState`, `T_WorkTableScanState`.

Each `ExecEnd*` function typically:
- Closes any open scan descriptors
- Frees node-specific resources (hash tables, sort states)
- Calls `ExecEndNode()` on child nodes
- Clears tuple table slots

---

### ExecReScan

#### Purpose

Resets a plan node so its output can be re-scanned, handling parameter changes
and propagating them to child nodes.

#### Signature

```c
/* Source: src/backend/executor/execAmi.c:68-310 */
void
ExecReScan(PlanState *node);
```

#### Step-by-Step Logic

1. **Instrumentation**: If collecting stats, call `InstrEndLoop()` to close the
   current loop's statistics.

2. **Parameter propagation**: If `chgParam` is set:
   - For each initPlan whose extParam overlaps with chgParam:
     call `UpdateChangedParamSet()` and possibly `ExecReScanSetParamPlan()`.
   - For each subPlan, propagate parameter changes.
   - Propagate to outer and inner child plan states.

3. **Reset ExprContext**: `ReScanExprContext()` resets the per-tuple memory context
   and calls any registered cleanup callbacks.

4. **Node-type-specific rescan**: Switch on NodeTag to call the appropriate
   `ExecReScan*` function (e.g., `ExecReScanSeqScan`, `ExecReScanHashJoin`).

5. **Clear chgParam**: Free the bitmapset and set to NULL.

#### Use Cases

- **Nested loop inner rescan**: The outer node of a NestLoop calls
  `ExecReScan()` on the inner node for each outer tuple (or when parameters
  change).
- **Merge join restart**: When the merge join needs to rescan the inner after
  processing a group of duplicate keys.
- **SubPlan re-evaluation**: Correlated subqueries need rescanning when
  outer parameters change.
- **Parameterized index scans**: When NestLoop passes new parameter values
  to a parameterized IndexScan.

---

### ExecShutdownNode

#### Purpose

Provides controlled shutdown for plan nodes, allowing them to stop asynchronous
operations and release resources (e.g., parallel workers) before `ExecEndNode`.

#### Signature

```c
/* Source: src/backend/executor/execProcnode.c:766-824 */
void ExecShutdownNode(PlanState *node);
```

This walks the plan tree using `planstate_tree_walker` and calls node-specific
shutdown functions for: Gather, GatherMerge, ForeignScan, CustomScan, Hash,
and HashJoin. It is called by `ExecutePlan()` when backward scan is not needed,
to release workers early.

---

### ExecSetTupleBound

#### Purpose

Propagates tuple count limits down through the plan tree to enable performance
optimizations in child nodes (e.g., top-N sort using a bounded heap).

#### Signature

```c
/* Source: src/backend/executor/execProcnode.c:842-982 */
void ExecSetTupleBound(int64 tuples_needed, PlanState *child_node);
```

#### Supported Node Types

The bound propagates through the following node types:

| Node Type | Effect |
|-----------|--------|
| `SortState` | Enables bounded (top-N) sort |
| `IncrementalSortState` | Enables bounded incremental sort |
| `AppendState` | Propagates to all children |
| `MergeAppendState` | Propagates to all children |
| `ResultState` | Propagates to outer child |
| `SubqueryScanState` | Propagates if no qual filter |
| `GatherState` | Sets `tuples_needed` and propagates to child |
| `GatherMergeState` | Sets `tuples_needed` and propagates to child |

---

## PlanState Structure

The PlanState is the runtime counterpart of the Plan node. Every plan node type
has a corresponding state type that "inherits" from PlanState.

```c
/* Source: src/include/nodes/execnodes.h:1113-1193 (partial) */
typedef struct PlanState
{
    NodeTag     type;

    Plan       *plan;               /* associated Plan node */
    EState     *state;              /* shared per-query execution state */

    ExecProcNodeMtd ExecProcNode;   /* function to return next tuple */
    ExecProcNodeMtd ExecProcNodeReal; /* actual function (unwrapped) */

    Instrumentation *instrument;    /* optional runtime stats */

    ExprState  *qual;               /* boolean qual condition */
    struct PlanState *lefttree;     /* outer (left) child */
    struct PlanState *righttree;    /* inner (right) child */

    List       *initPlan;           /* InitPlan SubPlanState nodes */
    List       *subPlan;            /* SubPlanState nodes in expressions */

    Bitmapset  *chgParam;           /* set of changed Param IDs */

    TupleDesc   ps_ResultTupleDesc; /* result tuple descriptor */
    TupleTableSlot *ps_ResultTupleSlot; /* result tuple slot */
    ExprContext *ps_ExprContext;    /* expression evaluation context */
    ProjectionInfo *ps_ProjInfo;   /* projection info */

    bool        async_capable;      /* true if async-capable */
    TupleDesc   scandesc;           /* scan slot descriptor hint */
} PlanState;
```

### Key Fields

| Field | Purpose |
|-------|---------|
| `ExecProcNode` | The visible function pointer called by `ExecProcNode()`. Initially set to `ExecProcNodeFirst`, then resolved on first call. |
| `ExecProcNodeReal` | The actual node-specific execution function (e.g., `ExecSeqScan`). |
| `qual` | Compiled qualification expression (WHERE clause fragment). Checked by `ExecQual()` during tuple processing. |
| `lefttree` / `righttree` | Links to child plan state nodes. By convention, lefttree = outer, righttree = inner. |
| `chgParam` | Bitmapset of PARAM_EXEC IDs that have changed. When non-NULL, `ExecProcNode()` calls `ExecReScan()` before fetching the next tuple. |
| `ps_ProjInfo` | If non-NULL, tuples are projected (target list evaluated) before being returned. |

---

## Pull-Based Execution: Concrete Example

Consider the query:
```sql
SELECT DEPT.no_emps, EMP.age
FROM DEPT, EMP
WHERE EMP.name = DEPT.mgr AND DEPT.name = 'shoe'
```

The planner might produce:
```
NestLoop (DEPT.mgr = EMP.name)
   /       \
SeqScan    SeqScan
  DEPT       EMP
(name='shoe')
```

**Initialization** (`ExecInitNode` called top-down):
1. `ExecInitNestLoop` is called for the root.
2. It calls `ExecInitNode` on the left child (DEPT SeqScan).
3. It calls `ExecInitNode` on the right child (EMP SeqScan).
4. Each node creates its PlanState, tuple slots, and expression state.

**Execution** (`ExecProcNode` called demand-driven):
1. `ExecutePlan` calls `ExecProcNode(NestLoopState)`.
2. `ExecNestLoop` calls `ExecProcNode` on the outer (DEPT) to get a tuple.
3. DEPT's `ExecSeqScan` calls `ExecScan` which fetches tuples from the heap.
4. `ExecScan` applies the qual `name = 'shoe'` via `ExecQual`.
5. When a qualifying outer tuple is found, `ExecNestLoop` rescans the inner.
6. For each inner tuple, `ExecNestLoop` checks the join qual `EMP.name = DEPT.mgr`.
7. Matching tuples are projected and returned up the tree.

**Cleanup** (`ExecEndNode` called bottom-up):
1. `ExecEndNestLoop` calls `ExecEndNode` on both children.
2. `ExecEndSeqScan` closes the heap scan and releases resources.

## Implementation Notes

- The use of function pointers for `ExecProcNode` was introduced to eliminate
  per-tuple switch dispatch overhead. Earlier PostgreSQL versions used a switch
  statement in `ExecProcNode` similar to `ExecInitNode`/`ExecEndNode`.
- `ExecInitNode` and `ExecEndNode` still use switch statements because they
  are called once per node (init) or once at end -- the per-call overhead is
  negligible.
- The `chgParam` mechanism is the executor's way of handling parameterized plans.
  When a parent node (e.g., NestLoop) sets a new parameter value, it adds the
  parameter ID to the child's `chgParam`. On the next `ExecProcNode` call,
  the child detects the change and rescans.
