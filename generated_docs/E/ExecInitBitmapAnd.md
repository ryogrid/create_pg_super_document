# ExecInitBitmapAnd

## Location
[src/backend/executor/nodeBitmapAnd.c:55-109](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeBitmapAnd.c#L55-L109)

## Overview
ExecInitBitmapAnd initializes a BitmapAndState node and all of its child subplan nodes to prepare for bitmap AND operations during query execution.

## Definition

```c
BitmapAndState *
ExecInitBitmapAnd(BitmapAnd *node, EState *estate, int eflags)
```
## Detailed Description
ExecInitBitmapAnd performs the initialization phase for BitmapAnd executor nodes. It creates and configures a BitmapAndState structure that will coordinate the execution of multiple bitmap-generating subplans. The function sets up an array of PlanState pointers for all child nodes and recursively initializes each subplan using ExecInitNode.

The function validates that unsupported execution flags (EXEC_FLAG_BACKWARD and EXEC_FLAG_MARK) are not set, as BitmapAnd nodes do not support backward scanning or mark/restore operations. It assigns ExecBitmapAnd as the ExecProcNode function pointer, though this function will error if called - the actual execution uses MultiExecBitmapAnd instead.

Unlike many other executor nodes, BitmapAnd nodes do not require expression contexts or tuple slots since they never call ExecQual or ExecProject - they only produce bitmap results by combining bitmaps from their children.

## Parameters / Member Variables
- : Pointer to the BitmapAnd plan node containing the list of subplans to be ANDed
- : Execution state context for the current query
- : Execution flags controlling behavior (EXEC_FLAG_BACKWARD and EXEC_FLAG_MARK are explicitly prohibited)

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (to create BitmapAndState)
  - [list_length](../l/list_length.md) (to count subplans)
  - [palloc0](../p/palloc0.md) (to allocate subplan state array)
  - [ExecInitNode](ExecInitNode.md) (to initialize each subplan)
  - [ExecBitmapAnd](ExecBitmapAnd.md) (assigned as ExecProcNode function)
- Called from (representative examples):
  - [ExecInitNode](ExecInitNode.md) (general node initialization dispatcher)

## Notes and Other Information
- Part of PostgreSQL's bitmap index scan optimization infrastructure
- Does not create expression contexts or tuple slots since BitmapAnd nodes don't process individual tuples
- The initialized node will later be executed via MultiExecBitmapAnd, not through the standard ExecProcNode interface
- Validates execution flags to ensure only supported operations are attempted
- Located in src/backend/executor/nodeBitmapAnd.c:55-109