# ExecInterpExpr

## Location
[src/backend/executor/execExprInterp.c:396-1914](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execExprInterp.c#L396-L1914)

## Overview
The core interpreter function that executes PostgreSQL expressions by evaluating a sequence of expression evaluation steps (opcodes) in a large switch statement or computed goto dispatch loop.

## Definition
```c
static Datum ExecInterpExpr(ExprState *state, ExprContext *econtext, bool *isnull)
```

## Detailed Description
ExecInterpExpr is PostgreSQL's expression interpreter that executes compiled expression trees represented as arrays of ExprEvalStep operations. It serves as the runtime engine for expression evaluation when JIT compilation is not available or not beneficial.

The function operates using either:
1. **Computed goto (EEO_USE_COMPUTED_GOTO)**: Uses GCC's computed goto extension for direct threading, where opcodes are replaced with jump addresses for maximum performance
2. **Traditional switch statement**: Falls back to a large switch statement for portability

Key characteristics:
- Handles over 80 different expression operation types (EEOP_* opcodes)
- Implements specialized handling for variables, constants, functions, boolean operations, type coercions, aggregates, and window functions
- Uses inline implementations for performance-critical operations
- Delegates complex operations to separate functions
- Supports conditional jumps for control flow (AND/OR short-circuiting, CASE expressions)

The interpreter processes each step sequentially, with each step potentially modifying the result value and null flag, until reaching EEOP_DONE.

## Parameters / Member Variables
- `state`: Pointer to ExprState containing the expression steps and metadata
- `econtext`: Expression context providing tuple slots and parameter values
- `isnull`: Output parameter set to true if the result is NULL

## Dependencies
- Functions called/Symbols referenced:
  - Various ExecEval* functions for complex operations (ExecEvalSysVar, ExecEvalWholeRowVar, etc.)
  - [CheckOpSlotCompatibility](../C/CheckOpSlotCompatibility.md) for slot validation
  - [slot_getsomeattrs](../s/slot_getsomeattrs.md) for tuple attribute fetching
  - FunctionCallInvoke for user-defined function calls
  - [MakeExpandedObjectReadOnlyInternal](../M/MakeExpandedObjectReadOnlyInternal.md) for object lifecycle management
- Called from:
  - [ExecReadyInterpretedExpr](ExecReadyInterpretedExpr.md) (as evalfunc_private)
  - [ExecInitInterpreter](ExecInitInterpreter.md) (for dispatch table initialization)

## Notes and Other Information
- The function contains a special case: when state is NULL, it returns the dispatch table address for computed goto initialization
- Performance is critical as this is in the hot path for all expression evaluation
- The large switch statement is organized by operation complexity, with simple operations like variable access inlined
- Supports both strict and non-strict function evaluation with NULL handling
- Implements SQL three-valued logic for boolean operations (TRUE/FALSE/NULL)
- The computed goto optimization can provide significant performance improvements on supported compilers

## Simplified Source

```c
// Simplified version of ExecInterpExpr - PostgreSQL's expression interpreter
static Datum ExecInterpExpr(ExprState *state, ExprContext *econtext, bool *isnull) {
    ExprEvalStep *op;
    TupleTableSlot *resultslot, *innerslot, *outerslot, *scanslot;

#if defined(EEO_USE_COMPUTED_GOTO)
    // Dispatch table for computed goto optimization
    static const void *const dispatch_table[] = {
        // ... opcodes mapped to jump addresses ...
    };

    // Special case: return dispatch table for initialization
    if (unlikely(state == NULL))
        return PointerGetDatum(dispatch_table);
#endif

    // Initialize execution state
    op = state->steps;
    resultslot = state->resultslot;
    innerslot = econtext->ecxt_innertuple;
    outerslot = econtext->ecxt_outertuple;
    scanslot = econtext->ecxt_scantuple;

    // Main interpreter loop
    while (true) {
        switch (op->opcode) {
            case EEOP_DONE:
                goto out;

            // Variable access operations
            case EEOP_INNER_VAR:
            case EEOP_OUTER_VAR:
            case EEOP_SCAN_VAR:
                // Fast path: direct access to pre-fetched tuple attributes
                *op->resvalue = slot->tts_values[op->d.var.attnum];
                *op->resnull = slot->tts_isnull[op->d.var.attnum];
                break;

            // Constant values
            case EEOP_CONST:
                *op->resvalue = op->d.constval.value;
                *op->resnull = op->d.constval.isnull;
                break;

            // Function calls
            case EEOP_FUNCEXPR:
                *op->resvalue = op->d.func.fn_addr(op->d.func.fcinfo_data);
                *op->resnull = op->d.func.fcinfo_data->isnull;
                break;

            case EEOP_FUNCEXPR_STRICT:
                // Check for NULL arguments in strict functions
                if (has_null_args(op->d.func.fcinfo_data)) {
                    *op->resnull = true;
                } else {
                    *op->resvalue = op->d.func.fn_addr(op->d.func.fcinfo_data);
                    *op->resnull = op->d.func.fcinfo_data->isnull;
                }
                break;

            // Boolean operations with short-circuiting
            case EEOP_BOOL_AND_STEP:
                if (*op->resnull || !DatumGetBool(*op->resvalue)) {
                    // Jump to end of AND expression if false/null
                    op = &state->steps[op->d.boolexpr.jumpdone];
                    continue;
                }
                break;

            case EEOP_BOOL_OR_STEP:
                if (!*op->resnull && DatumGetBool(*op->resvalue)) {
                    // Jump to end of OR expression if true
                    op = &state->steps[op->d.boolexpr.jumpdone];
                    continue;
                }
                break;

            // Conditional jumps
            case EEOP_JUMP_IF_NULL:
                if (*op->resnull) {
                    op = &state->steps[op->d.jump.jumpdone];
                    continue;
                }
                break;

            // Complex operations delegated to separate functions
            case EEOP_ARRAYEXPR:
                ExecEvalArrayExpr(state, op);
                break;

            case EEOP_SUBPLAN:
                ExecEvalSubPlan(state, op, econtext);
                break;

            // ... many more operation types ...

            default:
                // Handle other opcodes through delegation
                delegate_complex_operation(state, op, econtext);
                break;
        }

        // Advance to next step
        op++;
    }

out:
    *isnull = state->resnull;
    return state->resvalue;
}
```

Key simplifications made:
- Collapsed the massive switch statement into representative operation categories
- Focused on the core interpreter loop structure and dispatch mechanism
- Highlighted the computed goto optimization concept
- Simplified complex operations to show delegation pattern
- Maintained the essential control flow and variable access patterns
- Removed detailed implementation of 80+ opcodes for clarity while preserving the architecture