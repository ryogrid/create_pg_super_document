# ExecMakeFunctionResultSet

## Location
src/backend/executor/execSRF.c: 497 - 695

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
  - check_stack_depth (stack overflow protection)
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