# ExecIndexBuildScanKeys

## Location
[src/backend/executor/nodeIndexscan.c:1135-1640](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeIndexscan.c#L1135-L1640)

## Overview
ExecIndexBuildScanKeys builds index scan keys from index qualification expressions, converting various types of qualification clauses into ScanKey structures that can be used by index access methods.

## Definition

```c
structs: one per qual */
	n_scan_keys = list_length(quals);
```
## Detailed Description
ExecIndexBuildScanKeys processes index qualification expressions and converts them into ScanKey arrays that index access methods can use for scanning. The function handles five different types of index qualifications:

1. **Simple operator with constant** ("indexkey op constant") - Creates a ScanKey with the constant value
2. **Simple operator with expression** ("indexkey op expression") - Creates a ScanKey and sets up IndexRuntimeKeyInfo for runtime evaluation
3. **RowCompareExpr** ("(indexkey, indexkey, ...) op (expr, expr, ...)") - Creates header ScanKey plus subsidiary ScanKey array for multi-column comparisons
4. **ScalarArrayOpExpr** ("indexkey op ANY (array-expression)") - Handles array operations either through amsearcharray flag or IndexArrayKeyInfo structures
5. **NullTest** ("indexkey IS NULL/IS NOT NULL") - Creates ScanKey for null testing

The function also supports processing ORDER BY expressions for indexes that support amcanorderbyop, with identical behavior except for operator lookup methods.

The function dynamically manages memory for runtime keys and array keys, sharing the runtime keys array between indexquals and indexorderbys when processed in separate calls.

## Parameters / Member Variables
- : Executor state node for expression initialization
- : Index relation being scanned
- : List of index qualification expressions (indexquals or indexorderbys)
- : true if processing ORDER BY expressions, false for qualification expressions
- : Output pointer to receive array of ScanKeys
- : Output pointer to receive number of ScanKeys
- : Input/output pointer for IndexRuntimeKeyInfo array (may be pre-existing)
- : Input/output pointer for number of runtime keys
- : Output pointer to receive IndexArrayKeyInfo array (may be NULL)
- : Output pointer to receive number of array keys (may be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md)/palloc0/repalloc
  - [get_leftop](../g/get_leftop.md)/get_rightop
  - [get_op_opfamily_properties](../g/get_op_opfamily_properties.md)
  - [get_opfamily_proc](../g/get_opfamily_proc.md)
  - [ExecInitExpr](ExecInitExpr.md)
  - TypeIsToastable
  - [ScanKeyEntryInitialize](../S/ScanKeyEntryInitialize.md)
  - IndexRelationGetNumberOfKeyAttributes
  - MemSet
- Called from (representative examples):
  - [ExecInitIndexScan](ExecInitIndexScan.md) (in nodeIndexscan.c:967, 981)
  - [ExecInitBitmapIndexScan](ExecInitBitmapIndexScan.md) (in nodeBitmapIndexscan.c:268)
  - [ExecInitIndexOnlyScan](ExecInitIndexOnlyScan.md) (in nodeIndexonlyscan.c:600, 614)

## Notes and Other Information
- The function performs extensive validation of index qualifications including operator compatibility with index
- Runtime keys are dynamically resized using exponential growth strategy (starting at 8, doubling as needed)
- RowCompareExpr processing is restricted to B-tree indexes and creates subsidiary ScanKey structures
- ScalarArrayOpExpr handling depends on whether the index AM supports amsearcharray
- The function includes comprehensive error checking for malformed index qualifications
- Array keys are allocated optimistically and freed if unused
- Caller may pass NULL for arrayKeys/numArrayKeys to indicate array operations are not supported
- Located in src/backend/executor/nodeIndexscan.c:1135-1640