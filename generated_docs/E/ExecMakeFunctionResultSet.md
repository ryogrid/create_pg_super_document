# ExecMakeFunctionResultSet

## Location
[src/backend/executor/execSRF.c:497-695](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execSRF.c#L497-L695)

## Overview
Evaluates arguments and executes a set-returning function call for target list expressions, managing both ValuePerCall and Materialize protocols with proper state management across multiple result rows.

## Definition
```c
Datum ExecMakeFunctionResultSet(SetExprState *fcache, ExprContext *econtext, MemoryContext argContext, bool *isNull, ExprDoneCond *isDone)
```

## Detailed Description
ExecMakeFunctionResultSet is the execution engine for set-returning functions in SELECT target lists. It implements a sophisticated state machine that handles multiple invocation patterns:

1. **Tuplestore Continuation**: If a previous call resulted in a materialized tuplestore, continues reading rows from storage until exhausted
2. **Fresh Function Call**: Evaluates function arguments and invokes the function for the first time or after all previous results are consumed
3. **ValuePerCall Protocol**: Manages stateful functions that return one row per call, preserving argument values between invocations
4. **Materialize Protocol**: Handles functions that return all results at once in a tuplestore

Key features include:
- **Memory Management**: Arguments are evaluated in a long-lived context to support ValuePerCall functions that reference them across calls
- **State Preservation**: Uses setArgsValid flag to avoid re-evaluating arguments for subsequent ValuePerCall invocations
- **Protocol Validation**: Ensures functions follow the correct SRF protocol based on their declared behavior
- **Cleanup Management**: Registers shutdown callbacks for proper resource cleanup when expression contexts are destroyed

The function uses a restart mechanism to seamlessly transition between reading cached results and making new function calls.

## Parameters / Member Variables
- `fcache`: SetExprState containing function metadata, cached arguments, and result storage
- `econtext`: Expression context providing execution environment and memory contexts
- `argContext`: Long-lived memory context for function arguments (must survive across ValuePerCall invocations)
- `isNull`: Output parameter indicating if the returned Datum is NULL
- `isDone`: Output parameter indicating execution state (ExprSingleResult, ExprMultipleResult, or ExprEndResult)

## Dependencies
- Functions called/Symbols referenced:
  - [check_stack_depth](../c/check_stack_depth.md) (stack overflow protection)
  - [tuplestore_gettupleslot](../t/tuplestore_gettupleslot.md), tuplestore_end (tuplestore management)
  - [ExecFetchSlotHeapTupleDatum](ExecFetchSlotHeapTupleDatum.md), slot_getattr (tuple/scalar extraction)
  - [ExecEvalFuncArgs](ExecEvalFuncArgs.md) (argument evaluation)  
  - FunctionCallInvoke (function execution)
  - [pgstat_init_function_usage](../p/pgstat_init_function_usage.md), pgstat_end_function_usage (statistics tracking)
  - [RegisterExprContextCallback](../R/RegisterExprContextCallback.md), ShutdownSetExpr (cleanup management)
  - [ExecPrepareTuplestoreResult](ExecPrepareTuplestoreResult.md) (materialize protocol setup)
- Called from (representative examples):
  - [ExecProjectSRF](ExecProjectSRF.md) (src/backend/executor/nodeProjectSet.c:182)

## Notes and Other Information
- Designed specifically for nodeProjectSet.c which handles multiple SRFs in target lists
- Implements careful argument preservation for ValuePerCall functions to avoid memory corruption
- Uses goto restart pattern to efficiently handle transitions between cached and fresh results
- Supports both tuple-returning and scalar SRFs with appropriate result extraction logic
- Includes comprehensive protocol validation to catch SRF implementation errors
- The argContext parameter is critical for ValuePerCall functions - it must live longer than per-tuple contexts
- Registers cleanup callbacks only when necessary (for ValuePerCall functions) to avoid overhead
- Handles strict functions by skipping execution when NULL arguments are present, returning empty sets

## Simplified Source

```c
Datum
ExecMakeFunctionResultSet(SetExprState *fcache, ExprContext *econtext,
                         MemoryContext argContext, bool *isNull, ExprDoneCond *isDone)
{
    List *arguments;
    Datum result;
    FunctionCallInfo fcinfo;
    ReturnSetInfo rsinfo;
    bool callit;

restart:
    check_stack_depth();

    // Continue reading from existing tuplestore if available
    if (fcache->funcResultStore) {
        TupleTableSlot *slot = fcache->funcResultSlot;
        MemoryContext oldContext = MemoryContextSwitchTo(slot->tts_mcxt);
        bool foundTup = tuplestore_gettupleslot(fcache->funcResultStore, true, false,
                                               fcache->funcResultSlot);
        MemoryContextSwitchTo(oldContext);

        if (foundTup) {
            *isDone = ExprMultipleResult;
            if (fcache->funcReturnsTuple) {
                *isNull = false;
                return ExecFetchSlotHeapTupleDatum(fcache->funcResultSlot);
            } else {
                return slot_getattr(fcache->funcResultSlot, 1, isNull);
            }
        }

        // Tuplestore exhausted
        tuplestore_end(fcache->funcResultStore);
        fcache->funcResultStore = NULL;
        *isDone = ExprEndResult;
        *isNull = true;
        return (Datum) 0;
    }

    // Evaluate arguments if needed
    fcinfo = fcache->fcinfo;
    arguments = fcache->args;
    if (!fcache->setArgsValid) {
        MemoryContext oldContext = MemoryContextSwitchTo(argContext);
        ExecEvalFuncArgs(fcinfo, arguments, econtext);
        MemoryContextSwitchTo(oldContext);
    } else {
        fcache->setArgsValid = false;
    }

    // Set up result info for SRF protocol
    fcinfo->resultinfo = (Node *) &rsinfo;
    rsinfo.type = T_ReturnSetInfo;
    rsinfo.econtext = econtext;
    rsinfo.expectedDesc = fcache->funcResultDesc;
    rsinfo.allowedModes = (int) (SFRM_ValuePerCall | SFRM_Materialize);
    rsinfo.returnMode = SFRM_ValuePerCall;
    rsinfo.setResult = NULL;
    rsinfo.setDesc = NULL;

    // Check for NULL arguments in strict functions
    callit = true;
    if (fcache->func.fn_strict) {
        for (int i = 0; i < fcinfo->nargs; i++) {
            if (fcinfo->args[i].isnull) {
                callit = false;
                break;
            }
        }
    }

    // Call the function or handle strict function with NULLs
    if (callit) {
        fcinfo->isnull = false;
        rsinfo.isDone = ExprSingleResult;
        result = FunctionCallInvoke(fcinfo);
        *isNull = fcinfo->isnull;
        *isDone = rsinfo.isDone;
    } else {
        // Strict SRF with NULL args returns empty set
        result = (Datum) 0;
        *isNull = true;
        *isDone = ExprEndResult;
    }

    // Handle different SRF protocols
    if (rsinfo.returnMode == SFRM_ValuePerCall) {
        if (*isDone == ExprMultipleResult) {
            fcache->setArgsValid = true;
            if (!fcache->shutdown_reg) {
                RegisterExprContextCallback(econtext, ShutdownSetExpr,
                                          PointerGetDatum(fcache));
                fcache->shutdown_reg = true;
            }
        }
    } else if (rsinfo.returnMode == SFRM_Materialize) {
        if (rsinfo.setResult != NULL) {
            ExecPrepareTuplestoreResult(fcache, econtext,
                                      rsinfo.setResult, rsinfo.setDesc);
            goto restart;  // Start reading from tuplestore
        }
        // Empty result set
        *isDone = ExprEndResult;
        *isNull = true;
        result = (Datum) 0;
    }

    return result;
}
```