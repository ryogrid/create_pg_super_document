# ShutdownExprContext

## Location
[src/backend/executor/execUtils.c:954-994](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execUtils.c#L954-L994)

## Overview
ShutdownExprContext executes all registered shutdown callback functions in an ExprContext and cleans up the callback list, providing controlled cleanup of resources associated with expression evaluation.

## Definition
static void ShutdownExprContext(ExprContext *econtext, bool isCommit)

## Detailed Description
This static function is responsible for executing all callback functions registered in an ExprContext's callback list. It processes callbacks in reverse registration order (LIFO - Last In, First Out), which is important for proper cleanup sequencing where later-registered callbacks may depend on earlier ones.

The function switches to the ExprContext's per-tuple memory context before executing callbacks, ensuring that any memory leaked by callback functions will be automatically cleaned up when the per-tuple context is reset. This provides robust memory management even if individual callbacks are not perfectly written.

The isCommit parameter controls whether callbacks are actually executed or just cleaned up - when false, the callback list is emptied without executing the functions, which is used during error recovery scenarios.

## Parameters / Member Variables
- `econtext`: The ExprContext whose callbacks should be executed and cleaned up
- `isCommit`: If true, execute callbacks; if false, just clean up the list without execution

## Dependencies
- Functions called/Symbols referenced:
  - [ExprContext_CB](../E/ExprContext_CB.md) (callback structure type)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md) (memory context switching)
  - [pfree](../p/pfree.md) (memory deallocation)
- Called from (representative examples):
  - [FreeExprContext](../F/FreeExprContext.md) (during context destruction)
  - [ReScanExprContext](../R/ReScanExprContext.md) (during context reset/rescan)

## Notes and Other Information
This function is declared static, making it internal to execUtils.c. The reverse execution order (LIFO) is crucial for proper resource cleanup, as components registered later often depend on components registered earlier. The memory context switching ensures cleanup robustness even with poorly written callbacks that leak memory.