# Chapter 06 -- Expression Evaluation

**Prerequisites**: [Chapter 05 -- TupleTableSlot Abstraction](05_tuple_table_slot.md),
[Chapter 07 -- Memory Context Management](07_memory_context_management.md) (for per-tuple context)
**Next**: [Chapter 07 -- Memory Context Management](07_memory_context_management.md)

**Key symbols**: `ExprState`, `ExprEvalStep`, `ExecInitExpr`, `ExecInitExprRec`,
`ExecInitQual`, `ExecEvalExpr`, `ExecEvalExprSwitchContext`, `ExecQual`,
`ExecProject`, `ExecBuildProjectionInfo`, `ExecInterpExpr`, `ProjectionInfo`,
`ExprContext`

---

## Overview

The PostgreSQL executor evaluates expressions using a compiled step-based
pipeline. During initialization, expression trees (Expr nodes from the planner)
are "compiled" into flat arrays of `ExprEvalStep` operations. At runtime, a
step interpreter (or JIT-compiled function) walks through these steps
sequentially, executing each one.

This design provides several advantages over direct recursive tree walking:

- Flat step arrays have better cache locality than pointer-chasing through a tree.
- The interpreter can use computed goto (on GCC) for efficient dispatch.
- JIT compilation can convert the step array into native machine code.
- Common patterns (variable access, constant loading) map to single steps.

**Source files**:
- `src/backend/executor/execExpr.c` (4,560 lines) -- expression compilation
- `src/backend/executor/execExprInterp.c` (5,317 lines) -- step interpreter
- `src/include/nodes/execnodes.h` -- ExprState, ExprContext definitions
- `src/include/executor/execExpr.h` -- ExprEvalStep, opcode definitions

For a visual overview of the pipeline, see `diagrams/expression_pipeline.mermaid`.

---

## ExprState -- The Compiled Expression

```c
/* Source: src/include/nodes/execnodes.h:78 */
typedef struct ExprState
{
    NodeTag     type;
    uint8       flags;          /* EEO_FLAG_IS_QUAL, etc. */
    bool        resnull;        /* result null flag */
    Datum       resvalue;       /* result value */
    TupleTableSlot *resultslot; /* result slot (for projections) */
    struct ExprEvalStep *steps; /* compiled step array */
    ExprStateEvalFunc evalfunc; /* evaluation function pointer */
    Expr       *expr;           /* original expression (debugging) */
    void       *evalfunc_private; /* private state for evalfunc */
} ExprState;
```

The `evalfunc` pointer is the key dispatch mechanism. It is set to one of:

| Function | When Used |
|----------|-----------|
| `ExecInterpExpr` | Default interpreter |
| JIT-compiled function | When JIT compilation is enabled and cost thresholds are met |
| `ExecJustConst`, `ExecJustInnerVar`, etc. | Optimized fast-path for trivially simple expressions |

---

## Compilation: ExecInitExpr

### Purpose

Compiles an expression tree into an ExprState with a flat array of
ExprEvalStep operations. This is the entry point for all expression
compilation.

### Signature

```c
/* Source: src/backend/executor/execExpr.c:134 */
ExprState *
ExecInitExpr(Expr *node, PlanState *parent);
```

### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `node` | `Expr *` | Root of expression tree to compile (NULL returns NULL) |
| `parent` | `PlanState *` | Owning plan node (NULL for standalone evaluation) |

### Step-by-Step Logic

```c
ExprState *
ExecInitExpr(Expr *node, PlanState *parent)
{
    ExprState  *state;
    ExprEvalStep scratch = {0};

    if (node == NULL)
        return NULL;

    /* 1. Create ExprState */
    state = makeNode(ExprState);
    state->expr = node;
    state->parent = parent;

    /* 2. Generate setup steps (FETCHSOME for tuple deforming) */
    ExecCreateExprSetupSteps(state, (Node *) node);

    /* 3. Recursively compile expression tree into steps */
    ExecInitExprRec(node, state, &state->resvalue, &state->resnull);

    /* 4. Append termination step */
    scratch.opcode = EEOP_DONE;
    ExprEvalPushStep(state, &scratch);

    /* 5. Finalize: set evalfunc, optionally optimize */
    ExecReadyExpr(state);

    return state;
}
```

1. **Create ExprState**: Allocates the state node in the current memory context.
2. **Setup steps**: `ExecCreateExprSetupSteps()` scans the expression tree to
   determine which tuple slots need deforming and emits `EEOP_*_FETCHSOME`
   steps. These ensure attributes are extracted from physical tuples before
   any `EEOP_*_VAR` step accesses them. See
   [Chapter 05](05_tuple_table_slot.md#lazy-deforming) for deforming details.
3. **Recursive compilation**: `ExecInitExprRec()` walks the expression tree
   and generates one or more `ExprEvalStep` entries for each Expr node.
4. **Termination**: An `EEOP_DONE` step signals the interpreter to stop.
5. **Finalization**: `ExecReadyExpr()` resolves opcodes to interpreter
   dispatch addresses (for computed goto) and may select optimized evalfunc
   shortcuts for simple expressions.

---

## Recursive Compilation: ExecInitExprRec

### Purpose

The core recursive compiler that walks the expression tree and emits
`ExprEvalStep` operations for each expression node type.

### Signature

```c
/* Source: src/backend/executor/execExpr.c */
void
ExecInitExprRec(Expr *node, ExprState *state,
                Datum *resv, bool *resnull);
```

### Key Expression Types

| Expr Type | EEOP Opcode(s) | Description |
|-----------|----------------|-------------|
| `Var` | `EEOP_INNER_VAR`, `EEOP_OUTER_VAR`, `EEOP_SCAN_VAR` | Attribute access from tuple slots |
| `Const` | `EEOP_CONST` | Constant value loading |
| `Param` (EXTERN) | `EEOP_PARAM_EXTERN` | External parameter access |
| `Param` (EXEC) | `EEOP_PARAM_EXEC` | Internal parameter access |
| `FuncExpr` | `EEOP_FUNCEXPR`, `EEOP_FUNCEXPR_STRICT`, `EEOP_FUNCEXPR_FUSAGE` | Function call |
| `OpExpr` | Same as FuncExpr | Operators are implemented as functions |
| `BoolExpr` (AND) | `EEOP_BOOL_AND_STEP`, `EEOP_BOOL_AND_STEP_LAST` | Short-circuit AND |
| `BoolExpr` (OR) | `EEOP_BOOL_OR_STEP`, `EEOP_BOOL_OR_STEP_LAST` | Short-circuit OR |
| `SubPlan` | `EEOP_SUBPLAN` | Subquery evaluation |
| `CaseExpr` | `EEOP_CASE_TESTVAL`, `EEOP_JUMP_IF_NOT_TRUE` | CASE WHEN |
| `NullTest` | `EEOP_NULLTEST_ISNULL`, `EEOP_NULLTEST_ISNOTNULL` | IS NULL / IS NOT NULL |
| `ScalarArrayOpExpr` | `EEOP_SCALARARRAYOP` | ANY/ALL with arrays |
| `Aggref` | `EEOP_AGGREF` | Aggregate value reference |
| `WindowFunc` | `EEOP_WINDOW_FUNC` | Window function reference |

The `resv` and `resnull` parameters tell the compiler where to store the step's
result. For intermediate values, these point to workspace in the step array.
For the final result, they point to `state->resvalue` and `state->resnull`.

---

## Qualification: ExecInitQual and ExecQual

### ExecInitQual

Compiles a list of qualification expressions (implicitly ANDed) into a single
ExprState optimized for boolean evaluation.

```c
/* Source: src/backend/executor/execExpr.c */
ExprState *
ExecInitQual(List *qual, PlanState *parent);
```

Each qual expression is compiled with `ExecInitExprRec()`, followed by an
`EEOP_QUAL` step. The `EEOP_QUAL` step checks the result:

- If NULL or FALSE: immediately jumps to a final step returning FALSE
  (short-circuit).
- If TRUE: execution continues to the next qual.

The final ExprState has `EEO_FLAG_IS_QUAL` set in its flags.

### ExecQual

Evaluates a compiled qualification and returns a boolean result. This is the
primary function for WHERE clause and join condition evaluation.

```c
/* Source: src/include/executor/executor.h:413 */
static inline bool
ExecQual(ExprState *state, ExprContext *econtext)
{
    Datum       ret;
    bool        isnull;

    if (state == NULL)
        return true;          /* empty restriction = always true */

    Assert(state->flags & EEO_FLAG_IS_QUAL);

    ret = ExecEvalExprSwitchContext(state, econtext, &isnull);

    Assert(!isnull);          /* EEOP_QUAL never returns NULL */

    return DatumGetBool(ret);
}
```

Key behaviors:
- NULL state is treated as "true" (no restriction).
- Uses `ExecEvalExprSwitchContext()`, which switches to the per-tuple memory
  context before evaluation. See
  [Chapter 07](07_memory_context_management.md#execevalswitchcontext) for why.
- The `EEOP_QUAL` mechanism ensures the result is never NULL -- a NULL
  intermediate result is treated as FALSE.

---

## Evaluation Dispatch: ExecEvalExpr

The primary interface for evaluating a compiled expression:

```c
/* Source: src/include/executor/executor.h:333 */
static inline Datum
ExecEvalExpr(ExprState *state,
             ExprContext *econtext,
             bool *isNull)
{
    return state->evalfunc(state, econtext, isNull);
}
```

This inline function calls through the `evalfunc` pointer. The indirection
enables the expression system to select different evaluation strategies:

1. **`ExecInterpExpr`**: The default step-by-step interpreter
2. **JIT-compiled function**: Native code generated by LLVM
3. **Fast-path functions**: For trivially simple expressions (e.g., reading a
   single Var from a slot), specialized functions like `ExecJustInnerVar`
   avoid interpreter overhead entirely.

**Important**: A NULL ExprState can be passed to `ExecQual()` (treated as
"true"), but NOT to `ExecEvalExpr()`, which will crash on NULL.

---

## Projection: ExecProject and ExecBuildProjectionInfo

### ExecProject

Evaluates a target list and stores results in a result tuple slot. Used for
tuple projection (computing output columns from input tuples).

```c
/* Source: src/include/executor/executor.h:376 */
static inline TupleTableSlot *
ExecProject(ProjectionInfo *projInfo)
{
    ExprContext *econtext = projInfo->pi_exprContext;
    ExprState  *state = &projInfo->pi_state;
    TupleTableSlot *slot = state->resultslot;
    bool        isnull;

    ExecClearTuple(slot);
    (void) ExecEvalExprSwitchContext(state, econtext, &isnull);

    slot->tts_flags &= ~TTS_FLAG_EMPTY;
    slot->tts_nvalid = slot->tts_tupleDescriptor->natts;

    return slot;
}
```

The compiled projection expression writes results directly into the result
slot's `tts_values` and `tts_isnull` arrays. After evaluation, the slot is
marked as containing a valid virtual tuple. This is an inlined version of
`ExecStoreVirtualTuple()` for performance. See
[Chapter 05](05_tuple_table_slot.md) for slot details.

### ExecBuildProjectionInfo

Builds the `ProjectionInfo` structure that wraps a compiled target list:

```c
/* Source: src/backend/executor/execExpr.c */
ProjectionInfo *
ExecBuildProjectionInfo(List *targetList,
                        ExprContext *econtext,
                        TupleTableSlot *slot,
                        PlanState *parent,
                        TupleDesc inputDesc);
```

The `ProjectionInfo` structure:

```c
typedef struct ProjectionInfo
{
    NodeTag     type;
    ExprState   pi_state;       /* ExprState for projection evaluation */
    ExprContext *pi_exprContext; /* ExprContext for evaluation */
} ProjectionInfo;
```

### Identity Projection Optimization

When the target list is a simple identity mapping (every entry is a Var
referring to the same relation with the same attribute number),
`ExecAssignScanProjectionInfo` sets `ps_ProjInfo = NULL`. The scan node then
returns tuples without projection, avoiding unnecessary copying.

---

## Expression Interpretation: ExecInterpExpr

The default expression interpreter in `src/backend/executor/execExprInterp.c`
implements a step-by-step execution loop.

### Dispatch Mechanisms

**Computed goto (GCC)**:

When compiled with GCC, the interpreter uses computed goto:

```c
static const void *const dispatch_table[] = {
    &&CASE_EEOP_DONE,
    &&CASE_EEOP_INNER_FETCHSOME,
    &&CASE_EEOP_OUTER_FETCHSOME,
    /* ... one label per opcode ... */
};

#define EEO_DISPATCH() goto *((void *) op->opcode)
#define EEO_NEXT()     op++; EEO_DISPATCH()
```

This replaces opcodes with label addresses during `ExecReadyExpr()`,
eliminating the switch overhead. The CPU branch predictor performs better
because each dispatch point is a different indirect branch.

**Switch-based (standard C)**:

```c
#define EEO_DISPATCH() goto starteval
#define EEO_SWITCH()   starteval: switch ((ExprEvalOp) op->opcode)
#define EEO_CASE(name) case name:
#define EEO_NEXT()     op++; goto starteval
```

### Key Step Handlers

| Opcode | Action | Description |
|--------|--------|-------------|
| `EEOP_DONE` | Return resvalue/resnull | Terminates interpretation |
| `EEOP_INNER_FETCHSOME` | `slot_getsomeattrs(innerTupleSlot, N)` | Deform inner tuple |
| `EEOP_OUTER_FETCHSOME` | `slot_getsomeattrs(outerTupleSlot, N)` | Deform outer tuple |
| `EEOP_SCAN_FETCHSOME` | `slot_getsomeattrs(scanTupleSlot, N)` | Deform scan tuple |
| `EEOP_INNER_VAR` | Load from inner slot's Datum array | Direct array access |
| `EEOP_OUTER_VAR` | Load from outer slot's Datum array | Direct array access |
| `EEOP_SCAN_VAR` | Load from scan slot's Datum array | Direct array access |
| `EEOP_CONST` | Copy constant Datum and null flag | Trivial assignment |
| `EEOP_FUNCEXPR` | Call function via FmgrInfo | Full function call protocol |
| `EEOP_FUNCEXPR_STRICT` | Skip call if any arg is NULL | Common optimization |
| `EEOP_QUAL` | If NULL or FALSE, jump to end | Short-circuit for quals |
| `EEOP_JUMP` | Unconditional jump to target step | Control flow |
| `EEOP_JUMP_IF_NOT_TRUE` | Conditional jump | CASE WHEN, boolean logic |

The FETCHSOME steps integrate with the lazy deforming mechanism described in
[Chapter 05](05_tuple_table_slot.md#lazy-deforming).

---

## JIT Expression Compilation

PostgreSQL supports JIT compilation of expressions using LLVM. When enabled,
the expression evaluation function is compiled to native machine code.

### Activation

JIT compilation is threshold-based, controlled by GUC parameters:

| Parameter | Purpose |
|-----------|---------|
| `jit_above_cost` | Enable JIT for queries with cost above this threshold |
| `jit_optimize_above_cost` | Apply LLVM optimization passes above this cost |
| `jit_inline_above_cost` | Inline function calls above this cost |

The `es_jit_flags` field in `EState` controls which features are active.

### Process

1. `ExecReadyExpr()` is called during expression initialization.
2. If JIT is enabled, the step array is compiled to LLVM IR.
3. LLVM applies optimization passes based on the cost thresholds.
4. The compiled function replaces `ExecInterpExpr` as the `evalfunc` pointer.
5. Subsequent calls to `ExecEvalExpr()` execute native code directly.

### Benefits

- Eliminates interpreter dispatch overhead
- Enables inlining of simple functions (e.g., `int4eq`, `int4lt`)
- LLVM can optimize across step boundaries
- Tuple deforming can be compiled to direct memory access

---

## ExprContext -- Runtime Environment

The `ExprContext` provides the runtime environment for expression evaluation,
connecting expressions to their input data (tuples, parameters) and memory
management. See [Chapter 07](07_memory_context_management.md) for the
complete ExprContext coverage including memory lifecycle.

```c
/* Source: src/include/nodes/execnodes.h:251 */
typedef struct ExprContext
{
    NodeTag     type;

    /* Tuples that Var nodes may refer to */
    TupleTableSlot *ecxt_scantuple;
    TupleTableSlot *ecxt_innertuple;
    TupleTableSlot *ecxt_outertuple;

    /* Memory contexts */
    MemoryContext ecxt_per_query_memory;
    MemoryContext ecxt_per_tuple_memory;

    /* Parameter values */
    ParamExecData *ecxt_param_exec_vals;
    ParamListInfo ecxt_param_list_info;

    /* Aggregate values */
    Datum      *ecxt_aggvalues;
    bool       *ecxt_aggnulls;

    /* CASE test value */
    Datum       caseValue_datum;
    bool        caseValue_isNull;

    /* Link to containing EState */
    struct EState *ecxt_estate;

    /* Shutdown callbacks */
    ExprContext_CB *ecxt_callbacks;
} ExprContext;
```

### Tuple Slot References

The three tuple slot pointers determine which tuple a `Var` node refers to:

| Pointer | Maps From | Purpose |
|---------|-----------|---------|
| `ecxt_scantuple` | Other `varno` values | Base table row from scan node |
| `ecxt_innertuple` | `INNER_VAR` (-1) | Inner (right) tuple in a join |
| `ecxt_outertuple` | `OUTER_VAR` (-2) | Outer (left) tuple in a join |

During compilation, `ExecInitExprRec` maps `Var` nodes to the appropriate
slot based on `varno`, generating `EEOP_INNER_VAR`, `EEOP_OUTER_VAR`, or
`EEOP_SCAN_VAR` accordingly.

---

## The ExecScan Pattern

The generic scan loop in `src/backend/executor/execScan.c` demonstrates how
qualification and projection work together. This is the execution pattern
used by all 16 scan node types.

```c
/* Source: src/backend/executor/execScan.c:156 */
TupleTableSlot *
ExecScan(ScanState *node,
         ExecScanAccessMtd accessMtd,
         ExecScanRecheckMtd recheckMtd)
{
    ExprContext *econtext = node->ps.ps_ExprContext;
    ExprState  *qual = node->ps.qual;
    ProjectionInfo *projInfo = node->ps.ps_ProjInfo;

    /* Fast path: no qual, no projection */
    if (!qual && !projInfo)
    {
        ResetExprContext(econtext);
        return ExecScanFetch(node, accessMtd, recheckMtd);
    }

    ResetExprContext(econtext);

    for (;;)
    {
        TupleTableSlot *slot = ExecScanFetch(node, accessMtd, recheckMtd);

        if (TupIsNull(slot))
        {
            if (projInfo)
                return ExecClearTuple(projInfo->pi_state.resultslot);
            else
                return slot;
        }

        econtext->ecxt_scantuple = slot;

        if (qual == NULL || ExecQual(qual, econtext))
        {
            if (projInfo)
                return ExecProject(projInfo);
            else
                return slot;
        }
        else
            InstrCountFiltered1(node, 1);

        ResetExprContext(econtext);
    }
}
```

This pattern shows the three core expression operations:

1. **`ResetExprContext()`** -- free per-tuple memory from previous iteration.
   See [Chapter 07](07_memory_context_management.md#resetexprcontext----the-critical-per-tuple-operation).
2. **`ExecQual()`** -- evaluate WHERE clause.
3. **`ExecProject()`** -- compute output columns.

---

## Implementation Notes

- The expression compiler detects "simple Var" cases where a target list entry
  is a direct reference to an input column. These use specialized opcodes
  (`EEOP_ASSIGN_*_VAR`) that avoid function call overhead.

- The step array is allocated in the per-query memory context
  ([Chapter 07](07_memory_context_management.md)) and freed automatically when
  the context is destroyed. There is no `ExecEndExpr()` function.

- `ExecEvalExprSwitchContext()` switches to `ecxt_per_tuple_memory` before
  evaluation, ensuring palloc'd results (e.g., from text functions) are
  allocated in the short-lived per-tuple context. See
  [Chapter 07](07_memory_context_management.md#execevalswitchcontext).
