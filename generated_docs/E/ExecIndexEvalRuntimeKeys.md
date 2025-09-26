# ExecIndexEvalRuntimeKeys

## Location
[src/backend/executor/nodeIndexscan.c:599-660](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeIndexscan.c#L599-L660)

## Overview
Evaluates runtime key expressions and updates the scankeys with their computed values during index scan operations.

## Definition

```c
void
ExecIndexEvalRuntimeKeys(ExprContext *econtext,
						 IndexRuntimeKeyInfo *runtimeKeys, int numRuntimeKeys)
```
## Detailed Description
The  function is responsible for evaluating runtime key expressions and updating the corresponding scan keys with their computed values. This function is crucial for index scans where key values cannot be determined at plan time but must be computed during execution based on the current execution context.

The function operates by iterating through all runtime keys, evaluating each key expression using the current expression context, and storing the result in the appropriate scan key structure. It handles both NULL and non-NULL results appropriately, setting the SK_ISNULL flag for NULL values and handling TOAST decompression for potentially toasted values to avoid repeated detoasting during index operations.

All key values are allocated in the per-tuple memory context to ensure proper memory management throughout the scan operation.

## Parameters
- : The expression context containing the current execution state and variable values needed for expression evaluation
- : Array of IndexRuntimeKeyInfo structures containing the runtime key expressions and associated scan keys to be updated
- : The number of runtime keys in the runtimeKeys array

## Dependencies
- Functions called/Symbols referenced:
  - [ExecEvalExpr](ExecEvalExpr.md)
  - [PointerGetDatum](../P/PointerGetDatum.md)
  - PG_DETOAST_DATUM
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
- Data types used:
  - [IndexRuntimeKeyInfo](../I/IndexRuntimeKeyInfo.md)
  - ScanKey
  - [ExprContext](ExprContext.md)
- Constants used:
  - SK_ISNULL

## Called From
- [ExecReScanBitmapIndexScan](ExecReScanBitmapIndexScan.md) (src/backend/executor/nodeBitmapIndexscan.c:152)
- [ExecReScanIndexOnlyScan](ExecReScanIndexOnlyScan.md) (src/backend/executor/nodeIndexonlyscan.c:377)  
- [ExecReScanIndexScan](ExecReScanIndexScan.md) (src/backend/executor/nodeIndexscan.c:565)

## Notes and Other Information
- The function switches to per-tuple memory context to ensure key values have the appropriate lifetime
- Handles TOAST decompression proactively to avoid repeated detoasting during index operations
- Assumes that pass-by-reference values from outer scans will remain valid throughout the scan
- Sets or clears the SK_ISNULL flag appropriately based on whether the evaluated expression returns NULL
- Critical for parameterized and nested loop index scans where key values depend on outer query execution