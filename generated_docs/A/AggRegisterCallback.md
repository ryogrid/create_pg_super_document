# AggRegisterCallback

## Location
src/backend/executor/nodeAgg.c: 4654 - 4682

## Overview
Registers a cleanup callback for an aggregate function to ensure proper cleanup of non-memory resources before the aggregate context is reset.

## Definition
```c
void AggRegisterCallback(FunctionCallInfo fcinfo,
                        ExprContextCallbackFunction func,
                        Datum arg)
```

## Detailed Description
AggRegisterCallback allows aggregate functions to register cleanup callbacks that will be executed just before the associated aggregate context is reset. This mechanism is essential for proper resource management in aggregate functions that allocate non-memory resources such as tuplestores, tuplesorts, or maintain pins on slots.

The callback is triggered in two scenarios:
1. Between groups during aggregate processing
2. When rescanning the query

The callback will NOT be called on error paths, so aggregate functions should not rely on it for error cleanup. The timing is carefully designed so that the callback occurs after the final function result is no longer needed, making it safe for final functions to return data that will later be freed by the callback.

## Parameters / Member Variables
- `fcinfo`: FunctionCallInfo structure containing the aggregate execution context
- `func`: ExprContextCallbackFunction pointer to the cleanup function to be called
- `arg`: Datum argument to be passed to the callback function

## Dependencies
- Functions called/Symbols referenced:
  - IsA (macro for type checking)
  - [AggState](AggState.md) (aggregate execution state structure)
  - [RegisterExprContextCallback](../R/RegisterExprContextCallback.md) (registers the callback with the expression context)
  - elog (error logging function)
- Called from (representative examples):
  - [ordered_set_startup](../o/ordered_set_startup.md) (in orderedsetaggs.c)
  - AGG_CONTEXT_WINDOW (referenced in include/fmgr.h)

## Notes and Other Information
- Throws an ERROR if not called within a proper aggregate context
- The callback is registered with the current aggregate context (curaggcontext)
- Not currently useful for aggregates called as window functions
- Typical use cases include freeing tuplestores, tuplesorts, or releasing slot pins
- Callbacks are not executed on error paths - only during normal processing
- Safe for final functions to return data that will be freed by the registered callback
- The callback mechanism ensures proper resource cleanup between groups and during query rescans