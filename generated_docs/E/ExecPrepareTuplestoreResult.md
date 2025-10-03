# ExecPrepareTuplestoreResult

## Location
[src/backend/executor/execSRF.c:864-942](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execSRF.c#L864-L942)

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
  - [CreateTupleDescCopy](../C/CreateTupleDescCopy.md) (to copy tuple descriptors when needed)
  - [MakeSingleTupleTableSlot](../M/MakeSingleTupleTableSlot.md) (to create slot for reading tuplestore data)
  - [tupledesc_match](../t/tupledesc_match.md) (to validate tuple descriptor compatibility)
  - [FreeTupleDesc](../F/FreeTupleDesc.md) (to release dynamically-allocated tuple descriptors)
  - [RegisterExprContextCallback](../R/RegisterExprContextCallback.md) (to register cleanup callback)
  - [ShutdownSetExpr](../S/ShutdownSetExpr.md) (as the cleanup callback function)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md) (for memory context management)
- Called from (representative examples):
  - [ExecMakeFunctionResultSet](ExecMakeFunctionResultSet.md) (main caller for set-returning function execution)

## Notes and Other Information
- This is a static function, only accessible within the execSRF.c compilation unit
- The function handles both cases where the result descriptor is known in advance (sexpr->funcResultDesc) and where it's provided by the function
- Memory management is carefully handled, switching to the function's memory context when creating long-lived structures
- The function includes error handling for unsupported scenarios where type information cannot be determined
- Cleanup callback registration is idempotent - it only registers the callback once per SetExprState
- The TTSOpsMinimalTuple operations are used for the tuple table slot, optimizing for minimal memory overhead

## Simplified Source

```c
static void ExecPrepareTuplestoreResult(SetExprState *sexpr,
                                        ExprContext *econtext,
                                        Tuplestorestate *resultStore,
                                        TupleDesc resultDesc) {
    // Store the tuplestore for later row extraction
    sexpr->funcResultStore = resultStore;

    // Create result slot if not already done
    if (sexpr->funcResultSlot == NULL) {
        TupleDesc slotDesc;

        // Switch to function's memory context for persistent structures
        MemoryContext oldcontext = MemoryContextSwitchTo(sexpr->func.fn_mcxt);

        // Determine which tuple descriptor to use
        if (sexpr->funcResultDesc) {
            slotDesc = sexpr->funcResultDesc;
        } else if (resultDesc) {
            slotDesc = CreateTupleDescCopy(resultDesc);
        } else {
            // Error: cannot determine result type
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                           errmsg("function returning setof record called in "
                                  "context that cannot accept type record")));
        }

        // Create the slot for reading tuplestore data
        sexpr->funcResultSlot = MakeSingleTupleTableSlot(slotDesc, &TTSOpsMinimalTuple);
        MemoryContextSwitchTo(oldcontext);
    }

    // Validate tuple descriptor if provided by function
    if (resultDesc) {
        if (sexpr->funcResultDesc) {
            tupledesc_match(sexpr->funcResultDesc, resultDesc);
        }

        // Free dynamically-allocated descriptor to prevent leaks
        if (resultDesc->tdrefcount == -1) {
            FreeTupleDesc(resultDesc);
        }
    }

    // Register cleanup callback (once per SetExprState)
    if (!sexpr->shutdown_reg) {
        RegisterExprContextCallback(econtext, ShutdownSetExpr, PointerGetDatum(sexpr));
        sexpr->shutdown_reg = true;
    }
}
```