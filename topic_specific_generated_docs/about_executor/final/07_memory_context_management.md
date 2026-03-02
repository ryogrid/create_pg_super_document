# Chapter 07 -- Memory Context Management

**Prerequisites**: [Chapter 03 -- Executor Lifecycle](03_executor_lifecycle.md) (for EState),
[Chapter 06 -- Expression Evaluation](06_expression_evaluation.md) (for ExprContext usage)
**Next**: Chapter 08 -- Scan Node Infrastructure

**Key symbols**: `EState`, `ExprContext`, `CreateExecutorState`,
`FreeExecutorState`, `CreateExprContext`, `CreateWorkExprContext`,
`CreateStandaloneExprContext`, `ResetExprContext`, `ResetPerTupleExprContext`,
`ExecEvalExprSwitchContext`, `ExecAssignExprContext`, `FreeExprContext`

---

## Overview

The PostgreSQL executor uses a hierarchy of memory contexts to manage the
lifetimes of different categories of data. This hierarchy ensures that
short-lived data (per-tuple expression results) is freed promptly, while
long-lived data (plan state, hash tables) persists for the query's duration.

The correct use of these contexts is critical for both correctness (preventing
dangling pointers) and performance (preventing memory leaks in long-running
queries).

**Source files**:
- `src/backend/executor/execUtils.c` -- CreateExecutorState, CreateExprContext,
  FreeExecutorState, ResetExprContext
- `src/include/nodes/execnodes.h` -- EState, ExprContext definitions

## The Three-Level Hierarchy

```
  Caller's Context (typically PortalContext)
    |
    +-- es_query_cxt ("ExecutorState") -- per-query lifetime
          |
          +-- ExprContext #1 ecxt_per_tuple_memory ("ExprContext") -- reset per tuple
          +-- ExprContext #2 ecxt_per_tuple_memory ("ExprContext") -- reset per tuple
          +-- ...
          +-- HashTableContext -- per-node lifetime (hash join)
          +-- Tuplesort context -- per-node lifetime (sort)
          +-- AggContext -- per-group or per-query (aggregation)
          +-- JIT Context -- compiled expression code
```

| Level | Context | Lifetime | Contents |
|-------|---------|----------|----------|
| **Per-query** | `es_query_cxt` | Entire executor invocation | EState, PlanState nodes, ExprState step arrays, tuple descriptors |
| **Per-node** | Various | Until node cleanup | Hash tables, sort runs, tuplestore buffers |
| **Per-tuple** | `ecxt_per_tuple_memory` | One tuple cycle (reset each iteration) | Expression results, detoasted values, function return values |

---

## Per-Query Context: CreateExecutorState and FreeExecutorState

### CreateExecutorState

Creates and initializes the EState, which is the root of working storage for
an entire executor invocation.

```c
/* Source: src/backend/executor/execUtils.c:88 */
EState *
CreateExecutorState(void)
{
    EState     *estate;
    MemoryContext qcontext;
    MemoryContext oldcontext;

    /* Create per-query context as child of CurrentMemoryContext */
    qcontext = AllocSetContextCreate(CurrentMemoryContext,
                                     "ExecutorState",
                                     ALLOCSET_DEFAULT_SIZES);

    /* Allocate EState within the per-query context */
    oldcontext = MemoryContextSwitchTo(qcontext);
    estate = makeNode(EState);

    estate->es_query_cxt = qcontext;
    estate->es_direction = ForwardScanDirection;
    estate->es_snapshot = InvalidSnapshot;
    estate->es_processed = 0;
    estate->es_total_processed = 0;
    estate->es_exprcontexts = NIL;
    estate->es_per_tuple_exprcontext = NULL;
    estate->es_jit_flags = 0;
    estate->es_jit = NULL;
    /* ... other initializations ... */

    MemoryContextSwitchTo(oldcontext);
    return estate;
}
```

The EState is allocated inside its own per-query context. Destroying the
context via `MemoryContextDelete` also frees the EState itself -- no separate
pfree is needed.

The per-query context is created as a child of `CurrentMemoryContext` at the
time `CreateExecutorState()` is called. In the normal path through
`PortalRunSelect()`, this is the portal's memory context.

### FreeExecutorState

Releases the EState and all associated working storage.

```c
/* Source: src/backend/executor/execUtils.c:188 */
void
FreeExecutorState(EState *estate)
```

Steps:

1. **Shut down ExprContexts**: Iterate through `es_exprcontexts` and call
   `FreeExprContext()` on each. This ensures shutdown callbacks are invoked
   (e.g., releasing aggregate transition values, closing SPI connections).

2. **Release JIT context**: If `es_jit` is set, call `jit_release_context()`.

3. **Release partition directory**: If allocated, call
   `DestroyPartitionDirectory()`.

4. **Delete per-query context**: `MemoryContextDelete(estate->es_query_cxt)`
   frees all memory including the EState node itself.

Because all executor contexts are children of the per-query context, this
single delete frees everything: PlanState nodes, ExprState step arrays, tuple
descriptors, and all per-node contexts (hash tables, sort runs, etc.).

---

## Per-Tuple Context: ExprContext

### CreateExprContext

Creates an expression evaluation context within an EState. Each ExprContext
has its own per-tuple memory context that can be independently reset.

```c
/* Source: src/backend/executor/execUtils.c:303 */
ExprContext *
CreateExprContext(EState *estate)
{
    return CreateExprContextInternal(estate, ALLOCSET_DEFAULT_SIZES);
}
```

The internal implementation:

```c
/* Source: src/backend/executor/execUtils.c:233 */
static ExprContext *
CreateExprContextInternal(EState *estate, ...)
{
    ExprContext *econtext;
    MemoryContext oldcontext;

    /* Create ExprContext in per-query context */
    oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);
    econtext = makeNode(ExprContext);

    /* Initialize tuple slot pointers to NULL */
    econtext->ecxt_scantuple = NULL;
    econtext->ecxt_innertuple = NULL;
    econtext->ecxt_outertuple = NULL;

    /* Reference per-query context */
    econtext->ecxt_per_query_memory = estate->es_query_cxt;

    /* Create per-tuple context as child of per-query context */
    econtext->ecxt_per_tuple_memory =
        AllocSetContextCreate(estate->es_query_cxt,
                              "ExprContext", ...);

    /* Copy parameter references from EState */
    econtext->ecxt_param_exec_vals = estate->es_param_exec_vals;
    econtext->ecxt_param_list_info = estate->es_param_list_info;

    /* Link into EState's list (for cleanup) */
    estate->es_exprcontexts = lcons(econtext, estate->es_exprcontexts);

    MemoryContextSwitchTo(oldcontext);
    return econtext;
}
```

The `lcons()` call ensures ExprContexts are shut down in reverse order of
creation during `FreeExecutorState()`.

### CreateWorkExprContext

A variant that sizes the per-tuple memory context proportionally to
`work_mem`, preventing a single allocation from exceeding the work_mem limit:

```c
/* Source: src/backend/executor/execUtils.c:318 */
ExprContext *
CreateWorkExprContext(EState *estate)
{
    Size maxBlockSize = ALLOCSET_DEFAULT_MAXSIZE;

    /* Limit maxBlockSize to 1/16 of work_mem */
    while (16 * maxBlockSize > work_mem * 1024L)
        maxBlockSize >>= 1;

    if (maxBlockSize < ALLOCSET_DEFAULT_INITSIZE)
        maxBlockSize = ALLOCSET_DEFAULT_INITSIZE;

    return CreateExprContextInternal(estate, ...);
}
```

### CreateStandaloneExprContext

For expression evaluation outside a full executor invocation (e.g., index
expression evaluation, domain constraint checking):

```c
/* Source: src/backend/executor/execUtils.c:354 */
ExprContext *
CreateStandaloneExprContext(void)
```

Creates an ExprContext with `CurrentMemoryContext` as the parent and
`ecxt_estate = NULL`. The caller is responsible for cleanup.

### ExecAssignExprContext

Convenience function used during plan node initialization:

```c
void
ExecAssignExprContext(EState *estate, PlanState *planstate)
{
    planstate->ps_ExprContext = CreateExprContext(estate);
}
```

Most plan nodes call this during their `ExecInit*` function to create the
node's expression evaluation context.

---

## ResetExprContext -- The Critical Per-Tuple Operation

### Definition

```c
/* Source: src/include/executor/executor.h */
#define ResetExprContext(econtext) \
    MemoryContextReset((econtext)->ecxt_per_tuple_memory)
```

### Why Per-Tuple Reset is Critical

Without per-tuple context reset, memory allocated during expression evaluation
would accumulate over the query's lifetime. Consider:

```sql
SELECT * FROM t WHERE upper(name) = 'SMITH';
```

Each call to `upper()` allocates memory for the uppercased string. Without
reset, this would consume O(n) memory proportional to the number of rows
processed. With per-tuple reset, memory consumption is O(1) -- only the
current tuple's expression results are live at any time.

`MemoryContextReset()` frees all allocations within the context but keeps the
context itself and its initial block. This is much cheaper than deleting and
recreating a context.

### Where ResetExprContext is Called

1. **ExecutePlan loop** (`execMain.c`):
   ```c
   for (;;)
   {
       ResetPerTupleExprContext(estate);
       slot = ExecProcNode(planstate);
       ...
   }
   ```
   See [Chapter 03](03_executor_lifecycle.md#executeplan----the-inner-loop).

2. **ExecScan loop** (`execScan.c`):
   ```c
   ResetExprContext(econtext);
   for (;;)
   {
       slot = ExecScanFetch(node, accessMtd, recheckMtd);
       ...
       ResetExprContext(econtext);  /* after qual failure */
   }
   ```
   See [Chapter 06](06_expression_evaluation.md#the-execscan-pattern).

3. **Join nodes** -- before evaluating join quals on each new tuple combination.

4. **Aggregate nodes** -- between processing groups (sorted aggregation) or
   between probe tuples (hash aggregation).

### ResetPerTupleExprContext

Resets the EState-level per-tuple ExprContext used for constraints and index
computations:

```c
/* Source: src/include/executor/executor.h */
#define ResetPerTupleExprContext(estate) \
    do { \
        if ((estate)->es_per_tuple_exprcontext) \
            ResetExprContext((estate)->es_per_tuple_exprcontext); \
    } while (0)
```

The `es_per_tuple_exprcontext` is created on demand (lazily) by
`GetPerTupleExprContext()`.

---

## ExecEvalExprSwitchContext

Expression evaluation must happen in the per-tuple memory context so that any
allocations are automatically freed on the next tuple cycle:

```c
/* Source: src/include/executor/executor.h:348 */
static inline Datum
ExecEvalExprSwitchContext(ExprState *state,
                          ExprContext *econtext,
                          bool *isNull)
{
    Datum       retDatum;
    MemoryContext oldContext;

    oldContext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);
    retDatum = state->evalfunc(state, econtext, isNull);
    MemoryContextSwitchTo(oldContext);
    return retDatum;
}
```

This is used by `ExecQual()` and `ExecProject()` to ensure correct context
placement. See [Chapter 06](06_expression_evaluation.md#execqual) for usage.

Direct calls to `ExecEvalExpr()` (without the switch) should only be made
when the caller has already switched to the appropriate context.

---

## Memory Context Usage by Node Type

| Node Type | Additional Contexts | Purpose |
|-----------|-------------------|---------|
| HashJoin | HashTableContext, per-batch | Hash table memory; batch contexts for spill-to-disk |
| Sort | Tuplesort context | Sort runs and merge operations |
| Agg (hashed) | Hash aggregate context | Hash table for aggregate groups; may spill |
| Agg (sorted) | Per-group context | Reset between groups for transition values |
| WindowAgg | Per-partition context | Tuplestore for partition buffering |
| Material | Tuplestore context | Materialized tuple storage |
| Memoize | Cache context | Parameterized result cache |
| Gather | Per-worker contexts | Worker result collection |

---

## FreeExprContext and Shutdown Callbacks

```c
/* Source: src/backend/executor/execUtils.c:396 */
void
FreeExprContext(ExprContext *econtext, bool isCommit)
```

Before destroying an ExprContext, shutdown callbacks are invoked via
`ShutdownExprContext()`. These callbacks allow cleanup of resources beyond
memory:

- Aggregate transition values that hold external resources
- Open file descriptors from set-returning functions
- SPI connections

Callbacks are registered with `RegisterExprContextCallback()` and
automatically invoked during context shutdown.

---

## Memory Leak Prevention Patterns

### Pattern 1: ResetExprContext in Scan Loop

Every scan node that evaluates expressions must reset the per-tuple context
before processing each tuple:

```c
ResetExprContext(econtext);
for (;;)
{
    slot = fetch_next_tuple();
    if (TupIsNull(slot)) break;
    econtext->ecxt_scantuple = slot;

    if (ExecQual(qual, econtext))
        return ExecProject(projInfo);

    ResetExprContext(econtext);  /* free before next iteration */
}
```

### Pattern 2: Long-Lived Results Need Materialization

When a tuple must survive beyond the current per-tuple cycle (e.g., for
sorting, hashing, or cross-tuple comparisons), it must be materialized or
copied into a longer-lived context:

```c
MemoryContext old = MemoryContextSwitchTo(estate->es_query_cxt);
saved_tuple = ExecCopySlotHeapTuple(slot);
MemoryContextSwitchTo(old);

ResetExprContext(econtext);  /* safe: saved_tuple is in query context */
```

See [Chapter 05](05_tuple_table_slot.md#clearing-and-materializing) for
`ExecMaterializeSlot()` and `ExecCopySlotHeapTuple()`.

### Pattern 3: Aggregate Transition Values

Aggregate transition values are allocated in a per-group context that is NOT
the per-tuple context. The per-tuple context is reset between tuples, but
transition values must persist across all tuples in a group.

### Pattern 4: Node-Specific Work Memory

Nodes like HashJoin and Sort create their own memory contexts sized according
to `work_mem`. These contexts are children of the per-query context and are
destroyed either during `ExecEnd*` or when the per-query context is deleted.

---

## Implementation Notes

- For parallel query, each worker creates its own EState with its own
  per-query context. Worker ExprContexts are independent of the leader's.

- The per-query context name "ExecutorState" appears in memory context dumps
  (`pg_backend_memory_contexts` view), making it easy to identify
  executor-related memory consumption during debugging.

- Memory context reset (`MemoryContextReset`) is much cheaper than context
  deletion and recreation. Reset keeps the context's initial block allocation,
  avoiding repeated calls to `malloc`/`free`. This is why per-tuple contexts
  are reset rather than destroyed and recreated.
