# Expression Evaluation

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

Source files:
- `src/backend/executor/execExpr.c` (4,560 lines) -- expression compilation
- `src/backend/executor/execExprInterp.c` (5,317 lines) -- step interpreter
- `src/include/nodes/execnodes.h` -- ExprState, ExprContext definitions
- `src/include/executor/execExpr.h` -- ExprEvalStep, opcode definitions

## Key Concepts

- **ExprState**: The compiled representation of an expression. Contains the step
  array, the evaluation function pointer, and result storage.
- **ExprEvalStep**: A single operation in the compiled step array. Has an opcode
  (EEOP_*) and operation-specific data in a union.
- **ExprContext**: Runtime context providing tuple slots (scan, inner, outer),
  memory contexts, and parameter values for expression evaluation.
- **Compilation vs Interpretation**: `ExecInitExpr` compiles; `ExecInterpExpr`
  interprets. JIT can replace the interpreter.

## Architecture

```
See: diagrams/expression_pipeline.mermaid
```

## Core APIs

### ExprState

#### Definition

```c
/* Source: src/include/nodes/execnodes.h:78-120 */
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
    /* ... compilation-time fields ... */
} ExprState;
```

The `evalfunc` pointer is the key dispatch mechanism. It is set to one of:
- `ExecInterpExpr` -- the default interpreter
- A JIT-compiled function -- when JIT compilation is enabled and beneficial
- `ExecJustConst`, `ExecJustInnerVar`, etc. -- optimized fast-path functions
  for trivially simple expressions

---

### ExecInitExpr

#### Purpose

Compiles an expression tree into an ExprState with a flat array of ExprEvalStep
operations. This is the entry point for all expression compilation.

#### Signature

```c
/* Source: src/backend/executor/execExpr.c:100-163 */
ExprState *
ExecInitExpr(Expr *node, PlanState *parent);
```

#### Parameters

| Parameter | Type | Description | Constraints |
|-----------|------|-------------|-------------|
| `node` | `Expr *` | Root of expression tree to compile | May be NULL (returns NULL) |
| `parent` | `PlanState *` | Owning plan node | May be NULL for standalone |

#### Return Value

Returns an `ExprState *` ready for evaluation via `ExecEvalExpr()`, or NULL if
`node` is NULL.

#### Step-by-Step Logic

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
    state->ext_params = NULL;

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
   determine which tuple slots need deforming and emits `EEOP_*_FETCHSOME` steps.
3. **Recursive compilation**: `ExecInitExprRec()` walks the expression tree and
   generates one or more `ExprEvalStep` entries for each Expr node.
4. **Termination**: An `EEOP_DONE` step is appended to signal the interpreter to
   stop and return the result.
5. **Finalization**: `ExecReadyExpr()` resolves opcodes to interpreter dispatch
   addresses (for computed goto) and may select optimized evalfunc shortcuts.

#### Integration Points

- **Called by**: `ExecInitQual()`, `ExecBuildProjectionInfo()`, plan node
  initialization routines, constraint checking code
- **Calls**: `ExecCreateExprSetupSteps()`, `ExecInitExprRec()`, `ExecReadyExpr()`

---

### ExecInitExprRec

#### Purpose

The core recursive compiler that walks the expression tree and emits
`ExprEvalStep` operations for each expression node type. This is the largest
function in the expression compilation system.

#### Signature

```c
/* Source: src/backend/executor/execExpr.c (large function, ~3000 lines) */
void
ExecInitExprRec(Expr *node, ExprState *state,
                Datum *resv, bool *resnull);
```

#### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `node` | `Expr *` | Expression node to compile |
| `state` | `ExprState *` | Target ExprState accumulating steps |
| `resv` | `Datum *` | Where to store the result value |
| `resnull` | `bool *` | Where to store the result null flag |

#### Key Expression Types Handled

| Expr Type | EEOP Opcode(s) | Description |
|-----------|----------------|-------------|
| `Var` | `EEOP_INNER_VAR`, `EEOP_OUTER_VAR`, `EEOP_SCAN_VAR` | Attribute access from tuple slots |
| `Const` | `EEOP_CONST` | Constant value loading |
| `Param` (EXTERN) | `EEOP_PARAM_EXTERN` | External parameter access |
| `Param` (EXEC) | `EEOP_PARAM_EXEC` | Internal parameter access |
| `FuncExpr` | `EEOP_FUNCEXPR`, `EEOP_FUNCEXPR_STRICT`, `EEOP_FUNCEXPR_FUSAGE` | Function call |
| `OpExpr` | Same as FuncExpr | Operators are functions |
| `BoolExpr` (AND) | `EEOP_BOOL_AND_STEP`, `EEOP_BOOL_AND_STEP_LAST` | Short-circuit AND |
| `BoolExpr` (OR) | `EEOP_BOOL_OR_STEP`, `EEOP_BOOL_OR_STEP_LAST` | Short-circuit OR |
| `BoolExpr` (NOT) | `EEOP_BOOLTEST_IS_NOT_TRUE` | Boolean negation |
| `SubPlan` | `EEOP_SUBPLAN` | Subquery evaluation |
| `CaseExpr` | `EEOP_CASE_TESTVAL`, `EEOP_JUMP_IF_NOT_TRUE` | CASE WHEN |
| `NullTest` | `EEOP_NULLTEST_ISNULL`, `EEOP_NULLTEST_ISNOTNULL` | IS NULL / IS NOT NULL |
| `ScalarArrayOpExpr` | `EEOP_SCALARARRAYOP` | ANY/ALL with arrays |
| `Aggref` | `EEOP_AGGREF` | Aggregate value reference |
| `WindowFunc` | `EEOP_WINDOW_FUNC` | Window function reference |

---

### ExecInitQual

#### Purpose

Compiles a list of qualification expressions (implicitly ANDed) into a single
ExprState optimized for boolean evaluation. The compiled expression uses
`EEOP_QUAL` steps that provide short-circuit FALSE returns.

#### Signature

```c
/* Source: src/backend/executor/execExpr.c (after ExecInitExpr) */
ExprState *
ExecInitQual(List *qual, PlanState *parent);
```

#### Detailed Description

Each qual expression in the list is compiled with `ExecInitExprRec()`, followed
by an `EEOP_QUAL` step. The `EEOP_QUAL` step checks the result:
- If the result is NULL or FALSE, it immediately jumps to a final step that
  returns FALSE, short-circuiting the remaining quals.
- If TRUE, execution continues to the next qual.

The final ExprState has `EEO_FLAG_IS_QUAL` set in its flags, which `ExecQual()`
asserts is present.

---

### ExecEvalExpr

#### Purpose

The primary interface for evaluating a compiled expression. Dispatches to the
evaluation function stored in the ExprState.

#### Signature

```c
/* Source: src/include/executor/executor.h:333-339 */
static inline Datum
ExecEvalExpr(ExprState *state,
             ExprContext *econtext,
             bool *isNull)
{
    return state->evalfunc(state, econtext, isNull);
}
```

This inline function simply calls through the `evalfunc` pointer. The indirection
enables the expression system to select different evaluation strategies:

1. **ExecInterpExpr**: The default step-by-step interpreter
2. **JIT-compiled function**: Native code generated by LLVM
3. **Fast-path functions**: For trivially simple expressions (e.g., just reading
   a single Var from a slot), specialized functions like `ExecJustInnerVar` are
   used to avoid interpreter overhead entirely.

---

### ExecQual

#### Purpose

Evaluates a compiled qualification expression and returns a boolean result.
This is the primary function used for WHERE clause and join condition evaluation.

#### Signature

```c
/* Source: src/include/executor/executor.h:413-432 */
static inline bool
ExecQual(ExprState *state, ExprContext *econtext)
{
    Datum       ret;
    bool        isnull;

    /* Empty restriction list = always true */
    if (state == NULL)
        return true;

    Assert(state->flags & EEO_FLAG_IS_QUAL);

    ret = ExecEvalExprSwitchContext(state, econtext, &isnull);

    /* EEOP_QUAL should never return NULL */
    Assert(!isnull);

    return DatumGetBool(ret);
}
```

Key behaviors:
- NULL state is treated as "true" (no restriction).
- Uses `ExecEvalExprSwitchContext()` which switches to the per-tuple memory
  context before evaluation.
- The EEOP_QUAL mechanism ensures that the result is never NULL -- a NULL
  intermediate result is treated as FALSE by the EEOP_QUAL step.

---

### ExecProject

#### Purpose

Evaluates a target list and stores the results in a result tuple slot. Used for
tuple projection (computing output columns from input tuples).

#### Signature

```c
/* Source: src/include/executor/executor.h:376-401 */
static inline TupleTableSlot *
ExecProject(ProjectionInfo *projInfo)
{
    ExprContext *econtext = projInfo->pi_exprContext;
    ExprState  *state = &projInfo->pi_state;
    TupleTableSlot *slot = state->resultslot;
    bool        isnull;

    /* Clear result slot */
    ExecClearTuple(slot);

    /* Evaluate expression (fills slot's Datum/isnull arrays) */
    (void) ExecEvalExprSwitchContext(state, econtext, &isnull);

    /* Mark slot as containing a valid virtual tuple */
    slot->tts_flags &= ~TTS_FLAG_EMPTY;
    slot->tts_nvalid = slot->tts_tupleDescriptor->natts;

    return slot;
}
```

The compiled projection expression writes its results directly into the result
slot's `tts_values` and `tts_isnull` arrays. After evaluation, the slot is
marked as containing a valid virtual tuple. This is an inlined version of
`ExecStoreVirtualTuple()` for performance.

---

### ExecBuildProjectionInfo

#### Purpose

Builds a `ProjectionInfo` structure that compiles a target list into an ExprState
for projection. Detects the optimization case where the target list directly
matches the input tuple descriptor, allowing projection to be skipped.

#### Signature

```c
/* Source: src/backend/executor/execExpr.c */
ProjectionInfo *
ExecBuildProjectionInfo(List *targetList,
                        ExprContext *econtext,
                        TupleTableSlot *slot,
                        PlanState *parent,
                        TupleDesc inputDesc);
```

The `ProjectionInfo` structure contains:
```c
typedef struct ProjectionInfo
{
    NodeTag     type;
    ExprState   pi_state;       /* ExprState for projection evaluation */
    ExprContext *pi_exprContext; /* ExprContext for evaluation */
} ProjectionInfo;
```

When the target list is a simple identity mapping (every entry is a Var referring
to the same relation with the same attribute number), `ExecAssignScanProjectionInfo`
sets `ps_ProjInfo = NULL`, and the scan node returns tuples without projection.

---

## Expression Interpretation

### ExecInterpExpr

The default expression interpreter in `src/backend/executor/execExprInterp.c`
implements a step-by-step execution loop. It supports two dispatch mechanisms:

#### Computed Goto (GCC)

When compiled with GCC, the interpreter uses computed goto for dispatch:
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

This replaces opcodes with label addresses during `ExecReadyExpr()`, eliminating
the switch statement overhead entirely. The CPU branch predictor performs better
with computed goto because each dispatch point is a different indirect branch.

#### Switch-Based (Standard C)

On non-GCC compilers, a traditional switch statement is used:
```c
#define EEO_DISPATCH() goto starteval
#define EEO_SWITCH()   starteval: switch ((ExprEvalOp) op->opcode)
#define EEO_CASE(name) case name:
#define EEO_NEXT()     op++; goto starteval
```

### Key Step Handlers

| Opcode | Handler | Description |
|--------|---------|-------------|
| `EEOP_DONE` | Return resvalue/resnull | Terminates interpretation |
| `EEOP_INNER_FETCHSOME` | `slot_getsomeattrs(innerTupleSlot, N)` | Deform inner tuple |
| `EEOP_OUTER_FETCHSOME` | `slot_getsomeattrs(outerTupleSlot, N)` | Deform outer tuple |
| `EEOP_SCAN_FETCHSOME` | `slot_getsomeattrs(scanTupleSlot, N)` | Deform scan tuple |
| `EEOP_INNER_VAR` | Load value from inner slot's Datum array | Direct array access |
| `EEOP_OUTER_VAR` | Load value from outer slot's Datum array | Direct array access |
| `EEOP_SCAN_VAR` | Load value from scan slot's Datum array | Direct array access |
| `EEOP_CONST` | Copy constant Datum and null flag | Trivial assignment |
| `EEOP_FUNCEXPR` | Call function via FmgrInfo | Full function call protocol |
| `EEOP_FUNCEXPR_STRICT` | Skip call if any arg is NULL | Common optimization |
| `EEOP_QUAL` | If result is NULL or FALSE, jump to end | Short-circuit for quals |
| `EEOP_JUMP` | Unconditional jump to target step | Control flow |
| `EEOP_JUMP_IF_NOT_TRUE` | Conditional jump | CASE WHEN, boolean logic |

---

## JIT Expression Compilation

PostgreSQL supports JIT compilation of expressions using LLVM. When enabled,
the expression evaluation function is compiled to native machine code.

### Activation

JIT compilation is threshold-based, controlled by GUC parameters:
- `jit_above_cost`: Enable JIT for queries with cost above this threshold
- `jit_optimize_above_cost`: Apply LLVM optimization passes above this cost
- `jit_inline_above_cost`: Inline function calls above this cost

The `es_jit_flags` field in EState controls which JIT features are active.

### Process

1. `ExecReadyExpr()` is called during expression initialization.
2. If JIT is enabled, the step array is compiled to LLVM IR.
3. LLVM applies optimization passes based on the cost thresholds.
4. The compiled function replaces `ExecInterpExpr` as the `evalfunc` pointer.
5. Subsequent calls to `ExecEvalExpr()` execute native code directly.

### Benefits

- Eliminates interpreter dispatch overhead
- Enables inlining of simple functions (e.g., int4eq, int4lt)
- LLVM can optimize across step boundaries
- Tuple deforming can be compiled to direct memory access

---

## ExprContext

The ExprContext provides the runtime environment for expression evaluation,
connecting expressions to their input data (tuples, parameters) and memory
management.

```c
/* Source: src/include/nodes/execnodes.h:251-297 */
typedef struct ExprContext
{
    NodeTag     type;

    /* Tuples that Var nodes may refer to */
    TupleTableSlot *ecxt_scantuple;     /* INNER/OUTER/SCAN distinction */
    TupleTableSlot *ecxt_innertuple;    /* inner tuple for joins */
    TupleTableSlot *ecxt_outertuple;    /* outer tuple for joins */

    /* Memory contexts */
    MemoryContext ecxt_per_query_memory;  /* long-lived */
    MemoryContext ecxt_per_tuple_memory;  /* reset per tuple */

    /* Parameter values */
    ParamExecData *ecxt_param_exec_vals; /* PARAM_EXEC params */
    ParamListInfo ecxt_param_list_info;  /* external params */

    /* Aggregate values */
    Datum      *ecxt_aggvalues;          /* precomputed agg values */
    bool       *ecxt_aggnulls;           /* agg null flags */

    /* CASE test value */
    Datum       caseValue_datum;
    bool        caseValue_isNull;

    /* Domain constraint value */
    Datum       domainValue_datum;
    bool        domainValue_isNull;

    /* Link to containing EState */
    struct EState *ecxt_estate;

    /* Shutdown callbacks */
    ExprContext_CB *ecxt_callbacks;
} ExprContext;
```

### Tuple Slot References

The three tuple slot pointers determine which tuple a `Var` node refers to:
- `ecxt_scantuple`: The tuple from a scan node (base table row)
- `ecxt_innertuple`: The inner (right) tuple in a join
- `ecxt_outertuple`: The outer (left) tuple in a join

During expression compilation, Var nodes are mapped to one of these three slots
based on the Var's `varno`:
- `INNER_VAR` (-1) maps to `ecxt_innertuple`
- `OUTER_VAR` (-2) maps to `ecxt_outertuple`
- Other values map to `ecxt_scantuple`

---

## The ExecScan Pattern

The generic scan loop in `src/backend/executor/execScan.c` demonstrates how
qualification and projection work together. This is the execution pattern used
by all 16 scan node types.

```c
/* Source: src/backend/executor/execScan.c:134-254 */
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

        /* Place tuple in ExprContext for qual/projection evaluation */
        econtext->ecxt_scantuple = slot;

        /* Apply qualification */
        if (qual == NULL || ExecQual(qual, econtext))
        {
            if (projInfo)
                return ExecProject(projInfo);  /* project and return */
            else
                return slot;  /* return raw scan tuple */
        }
        else
            InstrCountFiltered1(node, 1);  /* count filtered rows */

        ResetExprContext(econtext);  /* free per-tuple memory */
    }
}
```

This pattern shows the three core expression operations:
1. `ResetExprContext()` -- free per-tuple memory from previous iteration
2. `ExecQual()` -- evaluate WHERE clause
3. `ExecProject()` -- compute output columns

## Implementation Notes

- A NULL ExprState can be passed to `ExecQual()` and `ExecCheck()` (treated as
  "true"), but NOT to `ExecEvalExpr()` which will crash on NULL.
- The expression compiler detects "simple Var" cases where a target list entry
  is just a direct reference to an input column. These are handled with
  specialized opcodes (`EEOP_ASSIGN_*_VAR`) that avoid function call overhead.
- The step array is allocated in the per-query memory context and freed
  automatically when the context is destroyed. There is no `ExecEndExpr()`
  function.
- `ExecEvalExprSwitchContext()` is a wrapper around `ExecEvalExpr()` that
  switches to `ecxt_per_tuple_memory` before evaluation. This ensures that
  any palloc'd results (e.g., from text functions) are allocated in the
  short-lived per-tuple context.
