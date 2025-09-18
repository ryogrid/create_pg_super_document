# UnregisterExprContextCallback

## Location
[src/backend/executor/execUtils.c:923-953](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execUtils.c#L923-L953)

## Overview
UnregisterExprContextCallback removes previously registered shutdown callback functions from an ExprContext, allowing cleanup of callbacks that are no longer needed.

## Definition
void UnregisterExprContextCallback(ExprContext *econtext, ExprContextCallbackFunction function, Datum arg)

## Detailed Description
This function searches through the linked list of callbacks stored in an ExprContext and removes all entries that match both the specified function pointer and argument value. It traverses the callback list maintained in econtext->ecxt_callbacks, comparing each callback's function and arg fields against the provided parameters. When a match is found, the callback node is unlinked from the list and freed. This mechanism allows for precise removal of specific callbacks without affecting other registered callbacks.

The function is typically used when a resource or operation that registered a cleanup callback is being disposed of earlier than the ExprContext itself, preventing unnecessary or potentially harmful callback execution during context shutdown.

## Parameters / Member Variables
- `econtext`: The ExprContext from which to remove the callback
- `function`: The callback function pointer to match for removal
- `arg`: The Datum argument that must match along with the function

## Dependencies
- Functions called/Symbols referenced:
  - ExprContext_CB (callback structure type)
  - [pfree](../p/pfree.md) (memory deallocation)
- Called from (representative examples):
  - [fmgr_sql](../f/fmgr_sql.md) (SQL function execution cleanup)
  - [end_MultiFuncCall](../e/end_MultiFuncCall.md) (multi-function call cleanup)
  - exec_rt_fetch (runtime tuple fetch cleanup)

## Notes and Other Information
This function performs an exact match on both the function pointer and argument value, ensuring that only the specific callback instance is removed. Multiple callbacks with the same function but different arguments will remain intact unless they specifically match both criteria. The function safely handles cases where the callback to be removed is not found in the list.