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
- [ScalarArrayOpExpr](../S/ScalarArrayOpExpr.md) handling depends on whether the index AM supports amsearcharray
- The function includes comprehensive error checking for malformed index qualifications
- Array keys are allocated optimistically and freed if unused
- Caller may pass NULL for arrayKeys/numArrayKeys to indicate array operations are not supported
- Located in src/backend/executor/nodeIndexscan.c:1135-1640

## Simplified Source

```c
void
ExecIndexBuildScanKeys(PlanState *planstate, Relation index,
                       List *quals, bool isorderby,
                       ScanKey *scanKeys, int *numScanKeys,
                       IndexRuntimeKeyInfo **runtimeKeys, int *numRuntimeKeys,
                       IndexArrayKeyInfo **arrayKeys, int *numArrayKeys)
{
    ListCell *qual_cell;
    ScanKey scan_keys;
    IndexRuntimeKeyInfo *runtime_keys;
    IndexArrayKeyInfo *array_keys;
    int n_scan_keys, n_runtime_keys, max_runtime_keys, n_array_keys;
    int j;

    // Allocate arrays for scan keys and supporting structures
    n_scan_keys = list_length(quals);
    scan_keys = palloc(n_scan_keys * sizeof(ScanKeyData));

    runtime_keys = *runtimeKeys;
    n_runtime_keys = max_runtime_keys = *numRuntimeKeys;

    array_keys = palloc0(n_scan_keys * sizeof(IndexArrayKeyInfo));
    n_array_keys = 0;

    // Process each qualification clause
    j = 0;
    foreach(qual_cell, quals)
    {
        Expr *clause = (Expr *) lfirst(qual_cell);
        ScanKey this_scan_key = &scan_keys[j++];

        if (IsA(clause, OpExpr)) {
            // Handle simple operator: indexkey op constant/expression
            process_simple_operator(clause, this_scan_key, index, planstate,
                                   isorderby, &runtime_keys, &n_runtime_keys, &max_runtime_keys);
        }
        else if (IsA(clause, RowCompareExpr)) {
            // Handle row comparison: (col1,col2) op (val1,val2)
            process_row_comparison(clause, this_scan_key, index, planstate,
                                 &runtime_keys, &n_runtime_keys, &max_runtime_keys);
        }
        else if (IsA(clause, ScalarArrayOpExpr)) {
            // Handle array operation: indexkey op ANY(array)
            process_array_operation(clause, this_scan_key, index, planstate,
                                  &runtime_keys, &n_runtime_keys, &max_runtime_keys,
                                  &array_keys, &n_array_keys);
        }
        else if (IsA(clause, NullTest)) {
            // Handle null test: indexkey IS [NOT] NULL
            process_null_test(clause, this_scan_key, index);
        }
        else {
            elog(ERROR, "unsupported indexqual type: %d", (int) nodeTag(clause));
        }
    }

    // Clean up unused arrays
    if (n_array_keys == 0) {
        pfree(array_keys);
        array_keys = NULL;
    }

    // Return results
    *scanKeys = scan_keys;
    *numScanKeys = n_scan_keys;
    *runtimeKeys = runtime_keys;
    *numRuntimeKeys = n_runtime_keys;
    if (arrayKeys) {
        *arrayKeys = array_keys;
        *numArrayKeys = n_array_keys;
    }
}
```