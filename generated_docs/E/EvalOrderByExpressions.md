# EvalOrderByExpressions

## Location
src/backend/executor/nodeIndexscan.c: 360 - 385

## Overview
The EvalOrderByExpressions function calculates the ORDER BY clause expressions based on the current heap tuple, storing the results in the IndexScanState for later comparison.

## Definition
```c
static void EvalOrderByExpressions(IndexScanState *node, ExprContext *econtext)
```

## Detailed Description
EvalOrderByExpressions is a utility function that evaluates all ORDER BY expressions for a given tuple during index scans. This function is crucial when the index access method provides potentially inaccurate ORDER BY values and a recheck is necessary:

1. **Memory Context Management**: Switches to the per-tuple memory context to ensure proper memory lifecycle management for expression evaluation results
2. **Expression Iteration**: Iterates through all ORDER BY expressions stored in node->indexorderbyorig
3. **Expression Evaluation**: Uses ExecEvalExpr to evaluate each ORDER BY expression against the current tuple in the expression context
4. **Result Storage**: Stores both the calculated values and null indicators in the IndexScanState arrays (iss_OrderByValues and iss_OrderByNulls)
5. **Context Restoration**: Restores the previous memory context to maintain proper memory management

The function provides the recalculated ORDER BY values that can be compared with the index-provided values to determine ordering accuracy and make reordering decisions.

## Parameters / Member Variables
- `node`: IndexScanState structure containing the ORDER BY expressions and result storage arrays
- `econtext`: Expression context containing the current tuple and evaluation environment

## Dependencies
- Functions called/Symbols referenced:
  - MemoryContextSwitchTo
  - ExecEvalExpr
  - lfirst (list iteration macro)
  - foreach (list iteration macro)
- Called from (representative examples):
  - ReorderTuple (nodeIndexscan.c:60)
  - IndexNextWithReorder (nodeIndexscan.c:292)

## Notes and Other Information
- This is a static function used internally within the index scan executor
- Essential for handling lossy index access methods that support ORDER BY operations
- Proper memory context switching ensures that expression evaluation results have the correct lifetime
- The function assumes that the iss_OrderByValues and iss_OrderByNulls arrays are properly allocated to match the number of ORDER BY expressions
- Works in conjunction with cmp_orderbyvals to determine if reordering is necessary
- The per-tuple memory context ensures that expression results are cleaned up appropriately between tuples