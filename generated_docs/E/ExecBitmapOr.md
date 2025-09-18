# ExecBitmapOr

## Location
[src/backend/executor/nodeBitmapOr.c:43-55](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeBitmapOr.c#L43-L55)

## Overview
ExecBitmapOr is a stub function that serves as pro forma compliance for the standard execution node interface but is not intended to be called directly.

## Definition


## Detailed Description
ExecBitmapOr is a placeholder function that implements the standard ExecProcNode interface for BitmapOr nodes, but it is not designed to be called. The function immediately raises an ERROR when invoked, indicating that BitmapOr nodes do not support the standard ExecProcNode call convention. Instead, BitmapOr nodes are executed through the MultiExecBitmapOr function, which follows the multi-execution interface pattern used for nodes that return bitmaps rather than tuple slots.

The function exists to satisfy the function pointer assignment in the node state structure during initialization, but any attempt to call it will result in a runtime error with the message "BitmapOr node does not support ExecProcNode call convention".

## Parameters / Member Variables
- : Pointer to the plan state structure (unused, as function immediately errors)

## Dependencies
- Functions called/Symbols referenced:
  - elog (for error reporting)
  - [BitmapOrState](../B/BitmapOrState.md) (structure type referenced in context)

- Called from (representative examples):
  - [ExecInitBitmapOr](ExecInitBitmapOr.md) (assigned as function pointer during initialization)

## Notes and Other Information
- This is a static function within nodeBitmapOr.c and is not exported
- The actual execution logic for BitmapOr nodes is implemented in MultiExecBitmapOr
- BitmapOr nodes use a different execution model than standard tuple-returning nodes
- The function serves as a safety mechanism to catch incorrect usage of the standard execution interface