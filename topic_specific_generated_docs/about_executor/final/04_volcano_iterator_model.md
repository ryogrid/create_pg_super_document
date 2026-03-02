# Chapter 04 -- Volcano Iterator Model

**Prerequisites**: [Chapter 03 -- Executor Lifecycle](03_executor_lifecycle.md)
**Next**: [Chapter 05 -- TupleTableSlot Abstraction](05_tuple_table_slot.md)

**Key symbols**: `ExecProcNode`, `ExecInitNode`, `ExecEndNode`,
`ExecSetExecProcNode`, `ExecProcNodeFirst`, `ExecProcNodeInstr`,
`MultiExecProcNode`, `ExecReScan`, `ExecShutdownNode`, `ExecSetTupleBound`,
`PlanState`

---

## Overview

PostgreSQL implements the Volcano (also called "iterator") execution model.
The central idea is that every plan node exposes the same tuple-at-a-time
interface:

- A parent node **pulls** tuples from its children by calling `ExecProcNode()`.
- Each call returns one `TupleTableSlot` or NULL to signal end-of-scan.
- This demand-driven approach provides composability: nodes combine freely
  into arbitrarily deep trees without any node needing to know what its
  parent or children are.

The dispatch functions live in `src/backend/executor/execProcnode.c` and
`src/backend/executor/execAmi.c`. The inline `ExecProcNode` function is
defined in `src/include/executor/executor.h`.

For a visual trace of tuple flow through a multi-node plan, see
`diagrams/volcano_tuple_flow.mermaid`. For the dispatch mechanism details,
see `diagrams/node_dispatch_flowchart.mermaid`.

## Three Phases of Execution

The Volcano model operates in three phases, each using a different dispatch
mechanism:

| Phase | Direction | Dispatch | Function |
|-------|-----------|----------|----------|
| **Init** | Top-down | Switch on NodeTag | `ExecInitNode()` |
| **Execute** | Demand-driven (pull) | Function pointer | `ExecProcNode()` |
| **Cleanup** | Bottom-up (recursive) | Switch on NodeTag | `ExecEndNode()` |

The switch dispatch for Init and End is acceptable because those functions
are called once per node. The function pointer dispatch for Execute eliminates
per-tuple switch overhead.

---

## PlanState -- The Runtime Node

Every Plan node from the planner has a corresponding PlanState node at
runtime. The PlanState tree mirrors the Plan tree structure but contains
runtime state: function pointers, compiled expressions, tuple slots, and
instrumentation.

```c
/* Source: src/include/nodes/execnodes.h (selected fields) */
typedef struct PlanState
{
    NodeTag     type;
    Plan       *plan;               /* associated Plan node */
    EState     *state;              /* shared per-query execution state */

    ExecProcNodeMtd ExecProcNode;   /* function to return next tuple */
    ExecProcNodeMtd ExecProcNodeReal; /* actual function (unwrapped) */

    Instrumentation *instrument;    /* optional runtime stats */

    ExprState  *qual;               /* compiled qual condition */
    struct PlanState *lefttree;     /* outer (left) child */
    struct PlanState *righttree;    /* inner (right) child */

    List       *initPlan;           /* InitPlan SubPlanState nodes */
    List       *subPlan;            /* SubPlanState nodes in expressions */

    Bitmapset  *chgParam;           /* set of changed Param IDs */

    TupleDesc   ps_ResultTupleDesc; /* result tuple descriptor */
    TupleTableSlot *ps_ResultTupleSlot; /* result tuple slot */
    ExprContext *ps_ExprContext;    /* expression evaluation context */
    ProjectionInfo *ps_ProjInfo;   /* projection info */
} PlanState;
```

| Field | Purpose |
|-------|---------|
| `ExecProcNode` | The visible function pointer called by `ExecProcNode()`. Initially set to `ExecProcNodeFirst`, then resolved on first call. |
| `ExecProcNodeReal` | The actual node-specific execution function (e.g., `ExecSeqScan`). |
| `qual` | Compiled qualification expression. Checked by `ExecQual()` during tuple processing. See [Chapter 06](06_expression_evaluation.md#execqual). |
| `lefttree` / `righttree` | Child plan state nodes. Convention: lefttree = outer, righttree = inner. |
| `chgParam` | Bitmapset of `PARAM_EXEC` IDs that have changed. When non-NULL, `ExecProcNode()` calls `ExecReScan()` before fetching. |
| `ps_ProjInfo` | If non-NULL, tuples are projected (target list evaluated) before return. See [Chapter 06](06_expression_evaluation.md#execproject). |
| `ps_ExprContext` | Expression evaluation context providing tuple slots and memory contexts. See [Chapter 07](07_memory_context_management.md). |

---

## ExecInitNode

### Purpose

Recursively initializes all nodes in a plan tree, dispatching on NodeTag to
call the appropriate `ExecInit*` function for each node type. Produces a
PlanState tree that mirrors the Plan tree.

### Signature

```c
/* Source: src/backend/executor/execProcnode.c:141 */
PlanState *
ExecInitNode(Plan *node, EState *estate, int eflags);
```

### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `node` | `Plan *` | Plan node to initialize (may be NULL for leaf) |
| `estate` | `EState *` | Shared per-query execution state |
| `eflags` | `int` | Capability flags (REWIND, BACKWARD, MARK, etc.) |

### Step-by-Step Logic

1. **NULL check**: If `node` is NULL, return NULL.

2. **Stack depth check**: `check_stack_depth()` prevents stack overflow on
   deeply nested plan trees.

3. **NodeTag dispatch**: A large switch statement dispatches to the
   appropriate ExecInit function:

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

   All 43 plan node types are handled. An unrecognized NodeTag produces an
   `elog(ERROR)`.

4. **Install execution wrapper**: `ExecSetExecProcNode(result, result->ExecProcNode)`
   wraps the execution function with `ExecProcNodeFirst` for stack checking
   and optional instrumentation.

5. **Initialize initPlans**: For each SubPlan in `node->initPlan`, call
   `ExecInitSubPlan()` to build the SubPlanState.

6. **Allocate instrumentation**: If `estate->es_instrument` is set, allocate
   an `Instrumentation` structure via `InstrAlloc()`. See
   [Chapter 03](03_executor_lifecycle.md#explain-analyze-instrumentation).

### What Each ExecInit* Function Does

Every node-specific initialization function follows the same pattern:

1. Create the node's state struct (e.g., `SeqScanState`)
2. Set `ExecProcNode` to the node's execution function (e.g., `ExecSeqScan`)
3. Call `ExecAssignExprContext()` to create the node's `ExprContext`
   (see [Chapter 07](07_memory_context_management.md))
4. Call `ExecInitNode()` on child nodes (lefttree, righttree)
5. Compile quals via `ExecInitQual()` (see [Chapter 06](06_expression_evaluation.md))
6. Create tuple slots via `ExecInitResultTupleSlotTL()` and/or
   `ExecInitScanTupleSlot()` (see [Chapter 05](05_tuple_table_slot.md))
7. Build projection info via `ExecBuildProjectionInfo()` or detect the
   identity-projection optimization

---

## ExecProcNode -- The Heart of Volcano

### Purpose

Returns the next tuple from a plan node. This is the most frequently called
function in the executor.

### Signature

```c
/* Source: src/include/executor/executor.h */
static inline TupleTableSlot *
ExecProcNode(PlanState *node)
{
    if (node->chgParam != NULL)  /* something changed? */
        ExecReScan(node);        /* let ReScan handle this */
    return node->ExecProcNode(node);
}
```

### Function Pointer Evolution

The `ExecProcNode` field in PlanState goes through three states:

1. **After `ExecInitNode`**: Set to `ExecProcNodeFirst` by
   `ExecSetExecProcNode()`.

2. **On first call**: `ExecProcNodeFirst()` checks stack depth, then:
   - If instrumented: sets pointer to `ExecProcNodeInstr`
   - Otherwise: sets pointer to `ExecProcNodeReal` (the actual node function)

3. **Subsequent calls**: Dispatches directly to the resolved function.

This design avoids both the overhead of a switch statement on every tuple
and the overhead of stack depth checking on every tuple (done only once).

### ExecSetExecProcNode

```c
/* Source: src/backend/executor/execProcnode.c:424 */
void
ExecSetExecProcNode(PlanState *node, ExecProcNodeMtd function)
{
    node->ExecProcNodeReal = function;
    node->ExecProcNode = ExecProcNodeFirst;
}
```

### Return Value

Returns a `TupleTableSlot *` containing the next tuple, or a slot with
`TTS_FLAG_EMPTY` set to signal end-of-scan. Use `TupIsNull(slot)` to test:

```c
#define TupIsNull(slot) ((slot) == NULL || TTS_EMPTY(slot))
```

### Performance Considerations

- The inline definition avoids function call overhead on the most
  performance-critical path in the executor.
- The `chgParam` check adds only a pointer comparison in the common case
  (no parameter changes).
- After the first call, the function pointer is fully resolved, eliminating
  further dispatch overhead.
- Earlier PostgreSQL versions used a switch statement in `ExecProcNode`
  similar to `ExecInitNode`/`ExecEndNode`. The function pointer approach
  was adopted to eliminate per-tuple switch overhead.

---

## MultiExecProcNode

### Purpose

Executes plan nodes that return complex data structures (hash tables, bitmaps)
rather than individual tuples. Only four node types support this interface.

### Signature

```c
/* Source: src/backend/executor/execProcnode.c:501 */
Node *
MultiExecProcNode(PlanState *node);
```

Unlike `ExecProcNode`, this uses a switch statement because only four node
types support it:

| NodeTag | Function | Returns |
|---------|----------|---------|
| `T_HashState` | `MultiExecHash()` | `HashJoinTable` |
| `T_BitmapIndexScanState` | `MultiExecBitmapIndexScan()` | `TIDBitmap` |
| `T_BitmapAndState` | `MultiExecBitmapAnd()` | `TIDBitmap` |
| `T_BitmapOrState` | `MultiExecBitmapOr()` | `TIDBitmap` |

Before dispatching, if `chgParam` is set, `ExecReScan()` is called to handle
parameter changes. `MultiExecProcNode` does not use `InstrStartNode`/`InstrStopNode`
because the nodes return complex structures rather than countable tuples.
Each node provides its own instrumentation.

---

## ExecEndNode

### Purpose

Recursively cleans up all nodes in the plan tree. After this call, the plan
cannot be processed further.

### Signature

```c
/* Source: src/backend/executor/execProcnode.c:556 */
void
ExecEndNode(PlanState *node);
```

### Logic

1. If `node` is NULL, return (leaf node).
2. Check stack depth.
3. Free `chgParam` bitmapset if present.
4. Switch on NodeTag to call the appropriate `ExecEnd*` function.

Three node types have no cleanup action and simply break:
`T_ValuesScanState`, `T_NamedTuplestoreScanState`, `T_WorkTableScanState`.

Each `ExecEnd*` function typically:
- Closes any open scan descriptors
- Frees node-specific resources (hash tables, sort states)
- Calls `ExecEndNode()` on child nodes (recursion)
- Clears tuple table slots via `ExecClearTuple()`

---

## ExecReScan

### Purpose

Resets a plan node so its output can be re-scanned, handling parameter changes
and propagating them to child nodes.

### Signature

```c
/* Source: src/backend/executor/execAmi.c:68 */
void
ExecReScan(PlanState *node);
```

### Step-by-Step Logic

1. **Instrumentation**: If collecting stats, call `InstrEndLoop()` to close
   the current loop's statistics.

2. **Parameter propagation**: If `chgParam` is set:
   - For each initPlan whose `extParam` overlaps with `chgParam`: call
     `UpdateChangedParamSet()` and possibly `ExecReScanSetParamPlan()`.
   - For each subPlan: propagate parameter changes.
   - Propagate to outer and inner child plan states.

3. **Reset ExprContext**: `ReScanExprContext()` resets the per-tuple memory
   context and invokes registered cleanup callbacks.

4. **Node-type-specific rescan**: Switch on NodeTag to call the appropriate
   `ExecReScan*` function (e.g., `ExecReScanSeqScan`, `ExecReScanHashJoin`).

5. **Clear chgParam**: Free the bitmapset and set to NULL.

### Use Cases

- **Nested loop inner rescan**: For each outer tuple in a NestLoop, the inner
  child is rescanned. If the inner is a parameterized index scan, the NestLoop
  sets the new parameter value and adds the parameter ID to the inner's
  `chgParam`. On the next `ExecProcNode` call, the inline check detects the
  change and calls `ExecReScan()`.

- **Merge join restart**: When processing duplicate keys, the merge join
  needs to rescan the inner from a previously marked position.

- **SubPlan re-evaluation**: Correlated subqueries rescan when outer
  parameters change.

### The chgParam Mechanism

The `chgParam` bitmapset in PlanState is the executor's mechanism for
handling parameterized plans:

1. A parent node (e.g., NestLoop) sets a new `PARAM_EXEC` value in
   `es_param_exec_vals`.
2. It adds the parameter ID to the child's `chgParam` via
   `UpdateChangedParamSet()`.
3. On the next `ExecProcNode` call, the inline `ExecProcNode()` function
   detects `chgParam != NULL` and calls `ExecReScan()`.
4. `ExecReScan()` propagates the change to grandchildren and resets the node.

---

## ExecShutdownNode

### Purpose

Provides controlled shutdown for plan nodes, allowing them to release resources
(especially parallel workers) before `ExecEndNode`.

### Signature

```c
/* Source: src/backend/executor/execProcnode.c:766 */
void ExecShutdownNode(PlanState *node);
```

This walks the plan tree using `planstate_tree_walker` and calls node-specific
shutdown functions for: Gather, GatherMerge, ForeignScan, CustomScan, Hash,
and HashJoin. It is called by `ExecutePlan()` when backward scan is not needed,
to release workers early.

---

## ExecSetTupleBound

### Purpose

Propagates tuple count limits down through the plan tree to enable performance
optimizations in child nodes.

### Signature

```c
/* Source: src/backend/executor/execProcnode.c:842 */
void ExecSetTupleBound(int64 tuples_needed, PlanState *child_node);
```

The bound propagates through these node types:

| Node Type | Effect |
|-----------|--------|
| SortState | Enables bounded (top-N) sort using a heap |
| IncrementalSortState | Enables bounded incremental sort |
| AppendState | Propagates to all children |
| MergeAppendState | Propagates to all children |
| ResultState | Propagates to outer child |
| SubqueryScanState | Propagates if no qual filter |
| GatherState | Sets `tuples_needed` and propagates to child |
| GatherMergeState | Sets `tuples_needed` and propagates to child |

This is how `LIMIT 10` on top of a `Sort` enables a top-N sort rather than
sorting the entire result set.

---

## Concrete Example: Pull-Based Execution

Consider the query:

```sql
SELECT dept.no_emps, emp.age
FROM dept, emp
WHERE emp.name = dept.mgr AND dept.name = 'shoe'
```

The planner might produce:

```
NestLoop (dept.mgr = emp.name)
   /       \
SeqScan    SeqScan
  dept       emp
(name='shoe')
```

### Initialization (top-down)

1. `ExecInitNestLoop` is called for the root.
2. It calls `ExecInitNode` on the left child (dept SeqScan).
3. It calls `ExecInitNode` on the right child (emp SeqScan).
4. Each node creates its PlanState, tuple slots, and compiled expressions.

### Execution (demand-driven pull)

1. `ExecutePlan` calls `ExecProcNode(NestLoopState)`.
2. `ExecNestLoop` calls `ExecProcNode` on the outer (dept) to get a tuple.
3. The dept `ExecSeqScan` calls `ExecScan`, which fetches tuples from the heap.
4. `ExecScan` applies the qual `name = 'shoe'` via `ExecQual()`.
   See [Chapter 06](06_expression_evaluation.md#execqual).
5. When a qualifying outer tuple is found, `ExecNestLoop` rescans the inner.
6. For each inner tuple, `ExecNestLoop` checks the join qual
   `emp.name = dept.mgr` via `ExecQual()`.
7. Matching tuples are projected via `ExecProject()` and returned up the tree.
   See [Chapter 06](06_expression_evaluation.md#execproject).

### Cleanup (bottom-up)

1. `ExecEndNestLoop` calls `ExecEndNode` on both children.
2. `ExecEndSeqScan` closes the heap scan and releases resources.

---

## Node Type Taxonomy

The 43 plan node types handled by `ExecInitNode` fall into these categories.
Each category is documented in its own chapter.

| Category | Node Types | Chapter |
|----------|-----------|---------|
| Scan (17) | SeqScan, SampleScan, IndexScan, IndexOnlyScan, BitmapIndexScan, BitmapHeapScan, TidScan, TidRangeScan, SubqueryScan, FunctionScan, TableFuncScan, ValuesScan, CteScan, NamedTuplestoreScan, WorkTableScan, ForeignScan, CustomScan | 08, 15-17 |
| Join (3) | NestLoop, MergeJoin, HashJoin | 10 |
| Aggregation (5) | Group, Agg, WindowAgg, Unique, SetOp | 11 |
| Modification (2) | ModifyTable, LockRows | 12 |
| Control (6) | Result, ProjectSet, Append, MergeAppend, RecursiveUnion, Limit | 19 |
| Parallel (2) | Gather, GatherMerge | 13 |
| Materialization (4) | Material, Sort, IncrementalSort, Memoize | 18 |
| Auxiliary (4) | Hash, BitmapAnd, BitmapOr | 10, 16 |
