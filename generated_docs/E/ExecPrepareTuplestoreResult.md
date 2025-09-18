# ExecPrepareTuplestoreResult

## Location
src/backend/executor/execSRF.c: 864 - 942

## Overview
ExecPrepareTuplestoreResult is a subroutine that prepares to extract rows from a tuplestore-based function result by setting up the necessary slot structures and validating tuple descriptors.

## Definition
```c
static void ExecPrepareTuplestoreResult(SetExprState *sexpr, ExprContext *econtext, Tuplestorestate *resultStore, TupleDesc resultDesc)
```

## Detailed Description
This function is specifically designed to prepare the infrastructure needed to extract rows from a tuplestore that contains the result of a set-returning function. It handles several critical tasks including creating and managing the funcResultSlot if needed, validating that the function returned the expected tuple descriptor, and ensuring proper cleanup through callback registration.

The function performs tuple descriptor validation by cross-checking the expected descriptor (from sexpr->funcResultDesc) against the actual descriptor returned by the function (resultDesc). For dynamically-allocated tuple descriptors, it also handles proper memory management by freeing descriptors that are no longer needed.

A key responsibility is ensuring that a cleanup callback (ShutdownSetExpr) is registered to handle resource cleanup if the operation is interrupted before completion.

## Parameters / Member Variables
- `sexpr`: Pointer to SetExprState containing the set expression's execution state
- `econtext`: Expression context for the current evaluation environment  
- `resultStore`: The tuplestore containing the function's result rows
- `resultDesc`: Tuple descriptor describing the structure of result tuples (may be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - CreateTupleDescCopy (to copy tuple descriptors when needed)
  - MakeSingleTupleTableSlot (to create slot for reading tuplestore data)
  - tupledesc_match (to validate tuple descriptor compatibility)
  - FreeTupleDesc (to release dynamically-allocated tuple descriptors)
  - RegisterExprContextCallback (to register cleanup callback)
  - ShutdownSetExpr (as the cleanup callback function)
  - MemoryContextSwitchTo (for memory context management)
- Called from (representative examples):
  - ExecMakeFunctionResultSet (main caller for set-returning function execution)

## Notes and Other Information
- This is a static function, only accessible within the execSRF.c compilation unit
- The function handles both cases where the result descriptor is known in advance (sexpr->funcResultDesc) and where it's provided by the function
- Memory management is carefully handled, switching to the function's memory context when creating long-lived structures
- The function includes error handling for unsupported scenarios where type information cannot be determined
- Cleanup callback registration is idempotent - it only registers the callback once per SetExprState
- The TTSOpsMinimalTuple operations are used for the tuple table slot, optimizing for minimal memory overhead