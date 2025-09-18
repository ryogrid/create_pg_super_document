# ExecInitBitmapIndexScan

## Location
src/backend/executor/nodeBitmapIndexscan.c: 202 - 321

## Overview
ExecInitBitmapIndexScan initializes a bitmap index scan node by setting up the execution state, opening the index relation, building scan keys, and preparing the index scan descriptor.

## Definition
```c
BitmapIndexScanState *ExecInitBitmapIndexScan(BitmapIndexScan *node, EState *estate, int eflags)
```

## Detailed Description
This function performs comprehensive initialization of a bitmap index scan node, which is used to collect tuple identifiers (TIDs) from an index for subsequent bitmap heap scan operations. The initialization process includes creating the execution state structure, opening the index relation with appropriate locking, building scan keys from the index qualification conditions, setting up expression contexts for runtime key evaluation, and initializing the index scan descriptor.

The function handles various execution modes including EXPLAIN-only operations, and properly manages runtime keys that need to be evaluated during execution (such as parameters from outer query levels) as well as array keys for IN-clause expressions. It assumes that an ancestor BitmapHeapScan node holds the necessary locks on the base relation.

## Parameters / Member Variables
- `node`: Pointer to BitmapIndexScan plan node containing index ID, scan relation ID, and qualification conditions
- `estate`: Execution state containing query context, snapshot, and runtime information
- `eflags`: Execution flags controlling behavior (EXEC_FLAG_EXPLAIN_ONLY, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (to create BitmapIndexScanState)
  - exec_rt_fetch (to get relation lock mode)
  - [index_open](../i/index_open.md) (to open the index relation)
  - [ExecIndexBuildScanKeys](ExecIndexBuildScanKeys.md) (to build scan keys from qualification)
  - ExecAssignExprContext (to set up expression context for runtime keys)
  - [index_beginscan_bitmap](../i/index_beginscan_bitmap.md) (to initialize bitmap index scan)
  - [index_rescan](../i/index_rescan.md) (to set initial scan keys if no runtime keys)
- Called from (representative examples):
  - [ExecInitNode](ExecInitNode.md) (from the general executor initialization framework)

## Notes and Other Information
- Part of the standard executor node lifecycle (Init -> Exec -> End)
- Supports EXPLAIN-only mode for index advisor plugins with nonexistent indexes
- Does not open or lock the base relation - assumes ancestor BitmapHeapScan handles this
- Sets ExecProcNode to ExecBitmapIndexScan (the stub function that throws an error)
- Creates separate expression context for runtime key evaluation if needed
- Handles both runtime keys (parameterized values) and array keys (IN clauses)
- Returns fully initialized BitmapIndexScanState ready for execution
- Located at src/backend/executor/nodeBitmapIndexscan.c:202-321