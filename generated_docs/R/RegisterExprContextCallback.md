# RegisterExprContextCallback

## Location
[src/backend/executor/execUtils.c:897-922](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execUtils.c#L897-L922)

## Overview
Registers a shutdown callback function in an ExprContext to provide cleanup hooks for functions that need resource cleanup during expression evaluation.

## Definition
```c
void RegisterExprContextCallback(ExprContext *econtext, ExprContextCallbackFunction function, Datum arg)
```

## Detailed Description
This function provides a mechanism for registering cleanup callbacks that will be executed when an ExprContext is deleted or rescanned. The callbacks are particularly useful for functions that return sets or allocate resources that need explicit cleanup. Callbacks are stored in a linked list and executed in reverse order of registration (LIFO), ensuring proper cleanup ordering. The callback information is allocated in the expression context's per-query memory context to ensure proper memory management. Note that callbacks will not be invoked if execution is aborted due to an error - they only run during normal cleanup operations.

## Parameters / Member Variables
- `econtext`: Expression context in which to register the callback
- `function`: Callback function to be called during cleanup (of type ExprContextCallbackFunction)
- `arg`: Datum argument to pass to the callback function

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md)
  - [ExprContext_CB](../E/ExprContext_CB.md) (structure type)
- Called from (representative examples):
  - [ExecMakeFunctionResultSet](../E/ExecMakeFunctionResultSet.md)
  - [ExecPrepareTuplestoreResult](../E/ExecPrepareTuplestoreResult.md)
  - [fmgr_sql](../f/fmgr_sql.md)
  - [AggRegisterCallback](../A/AggRegisterCallback.md)
  - [init_MultiFuncCall](../i/init_MultiFuncCall.md)

## Notes and Other Information
- Callbacks are executed in LIFO (last-in, first-out) order during cleanup
- Memory for callback records is allocated in the per-query memory context
- Primarily used by set-returning functions and aggregate functions for resource cleanup
- Callbacks are not invoked during error abort scenarios - only during normal cleanup
- Essential for functions that maintain state or resources across multiple invocations
- Part of PostgreSQL's expression evaluation cleanup infrastructure