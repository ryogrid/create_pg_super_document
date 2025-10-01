# ExecInitBitmapIndexScan

## Location
[src/backend/executor/nodeBitmapIndexscan.c:202-321](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeBitmapIndexscan.c#L202-L321)

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
  - [exec_rt_fetch](../e/exec_rt_fetch.md) (to get relation lock mode)
  - [index_open](../i/index_open.md) (to open the index relation)
  - [ExecIndexBuildScanKeys](ExecIndexBuildScanKeys.md) (to build scan keys from qualification)
  - [ExecAssignExprContext](ExecAssignExprContext.md) (to set up expression context for runtime keys)
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

## Simplified Source

```c
BitmapIndexScanState *
ExecInitBitmapIndexScan(BitmapIndexScan *node, EState *estate, int eflags)
{
    BitmapIndexScanState *indexstate;
    LOCKMODE lockmode;

    // Validation
    Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

    // Create and initialize state structure
    indexstate = makeNode(BitmapIndexScanState);
    indexstate->ss.ps.plan = (Plan *) node;
    indexstate->ss.ps.state = estate;
    indexstate->ss.ps.ExecProcNode = ExecBitmapIndexScan;

    // Initialize bitmap result (created at runtime)
    indexstate->biss_result = NULL;

    // Base relation is handled by ancestor BitmapHeapScan
    indexstate->ss.ss_currentRelation = NULL;
    indexstate->ss.ss_currentScanDesc = NULL;

    // Early return for EXPLAIN only
    if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
        return indexstate;

    // Open the index relation
    lockmode = exec_rt_fetch(node->scan.scanrelid, estate)->rellockmode;
    indexstate->biss_RelationDesc = index_open(node->indexid, lockmode);

    // Initialize scan key state
    indexstate->biss_RuntimeKeysReady = false;
    indexstate->biss_RuntimeKeys = NULL;
    indexstate->biss_NumRuntimeKeys = 0;

    // Build scan keys from index qualification
    ExecIndexBuildScanKeys((PlanState *) indexstate,
                           indexstate->biss_RelationDesc,
                           node->indexqual,
                           false,
                           &indexstate->biss_ScanKeys,
                           &indexstate->biss_NumScanKeys,
                           &indexstate->biss_RuntimeKeys,
                           &indexstate->biss_NumRuntimeKeys,
                           &indexstate->biss_ArrayKeys,
                           &indexstate->biss_NumArrayKeys);

    // Setup expression context for runtime keys if needed
    if (indexstate->biss_NumRuntimeKeys != 0 || indexstate->biss_NumArrayKeys != 0) {
        ExprContext *stdecontext = indexstate->ss.ps.ps_ExprContext;
        ExecAssignExprContext(estate, &indexstate->ss.ps);
        indexstate->biss_RuntimeContext = indexstate->ss.ps.ps_ExprContext;
        indexstate->ss.ps.ps_ExprContext = stdecontext;
    } else {
        indexstate->biss_RuntimeContext = NULL;
    }

    // Initialize scan descriptor
    indexstate->biss_ScanDesc =
        index_beginscan_bitmap(indexstate->biss_RelationDesc,
                               estate->es_snapshot,
                               indexstate->biss_NumScanKeys);

    // Set scan keys if no runtime evaluation needed
    if (indexstate->biss_NumRuntimeKeys == 0 && indexstate->biss_NumArrayKeys == 0) {
        index_rescan(indexstate->biss_ScanDesc,
                     indexstate->biss_ScanKeys, indexstate->biss_NumScanKeys,
                     NULL, 0);
    }

    return indexstate;
}
```