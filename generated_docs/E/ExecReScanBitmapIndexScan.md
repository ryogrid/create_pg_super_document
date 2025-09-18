# ExecReScanBitmapIndexScan

## Location
src/backend/executor/nodeBitmapIndexscan.c: 131 - 174

## Overview
ExecReScanBitmapIndexScan recalculates runtime-dependent scan key values and resets the bitmap index scan to start over with potentially new search conditions.

## Definition
```c
void ExecReScanBitmapIndexScan(BitmapIndexScanState *node)
```

## Detailed Description
This function handles the rescan operation for bitmap index scans, which is necessary when scan parameters need to be re-evaluated based on runtime information, such as values from outer query levels in nested loops. The function performs several key operations: it resets the expression context to prevent memory leaks, recalculates any runtime keys whose values depend on information known only at execution time, evaluates array keys for IN-clause expressions, and finally resets the underlying index scan with the new key values.

The function is particularly important for handling parameterized scans where scan keys reference values from outer query blocks, and for array keys that represent IN-clause conditions where the set of values may change between executions.

## Parameters / Member Variables
- `node`: Pointer to BitmapIndexScanState containing the execution state, including runtime context, scan keys, and scan descriptor

## Dependencies
- Functions called/Symbols referenced:
  - ResetExprContext (to prevent memory leaks in runtime context)
  - ExecIndexEvalRuntimeKeys (to recalculate runtime-dependent scan keys)
  - ExecIndexEvalArrayKeys (to evaluate array keys for IN clauses)
  - index_rescan (to restart the index scan with new keys)
- Called from (representative examples):
  - ExecReScan (from the general executor rescan framework)

## Notes and Other Information
- Essential for nested loop joins where inner scan parameters depend on outer tuple values
- Handles both regular runtime keys and array keys for IN-clause expressions
- Sets biss_RuntimeKeysReady flag to indicate whether keys are properly evaluated
- If array key evaluation results in an empty set, biss_RuntimeKeysReady remains false, indicating no scan is needed
- Memory management is carefully handled through expression context reset
- Only calls index_rescan if runtime keys are successfully prepared
- Located at src/backend/executor/nodeBitmapIndexscan.c:131-174