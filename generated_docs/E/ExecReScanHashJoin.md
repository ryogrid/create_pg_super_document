# ExecReScanHashJoin

## Location
src/backend/executor/nodeHashjoin.c: 1395 - 1482

## Overview
Resets and rescans a hash join node, handling both hash table reuse optimization for single-batch joins and complete reconstruction for multi-batch or parameter-changed scenarios.

## Definition
```c
void ExecReScanHashJoin(HashJoinState *node)
```

## Detailed Description
This function implements the rescan logic for hash join operations, providing an important optimization for single-batch joins where the hash table can be reused. It evaluates whether the existing hash table can be preserved (single-batch join with no parameter changes), handles different join types by resetting match flags for right/anti/full joins when reusing tables, manages instrumentation and statistics collection before destroying hash tables, resets all intra-tuple state variables to their initial values, and coordinates rescanning of child plan nodes when parameters haven't changed.

The function implements a key optimization where single-batch hash tables are reused when possible, avoiding expensive rebuild operations. For multi-batch joins or when parameters change, it properly destroys the old hash table and sets up for rebuilding.

## Parameters / Member Variables
- `node`: The HashJoinState containing the hash join execution state, hash table reference, join state, and links to child plan nodes

## Dependencies
- Functions called/Symbols referenced:
  - outerPlanState
  - innerPlanState
  - HJ_FILL_INNER
  - ExecHashTableResetMatchFlags
  - castNode
  - palloc0
  - ExecHashAccumInstrumentation
  - ExecHashTableDestroy
  - ExecReScan
- Called from (representative examples):
  - ExecReScan (generic executor rescan dispatcher)

## Notes and Other Information
- Implements hash table reuse optimization for single-batch joins without parameter changes
- Handles instrumentation collection before destroying hash tables to preserve performance statistics
- Resets join state to either HJ_NEED_NEW_OUTER (reusing table) or HJ_BUILD_HASHTABLE (rebuilding)
- Always resets intra-tuple state regardless of hash table reuse decision
- For multi-batch joins, currently requires complete rescan due to potential release of batch temp files
- Properly coordinates with child plan rescans, respecting parameter change indicators
- Function is declared in nodeHashjoin.h and used by the general executor rescan mechanism