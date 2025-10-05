# _bt_compare_array_scankey_args

## Location
[src/backend/access/nbtree/nbtutils.c:976-1098](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtutils.c#L976-L1098)

## Overview
Compares an array scan key against a scalar scan key to eliminate contradictory array elements, making the scalar scan key redundant when possible.

## Definition

```c
static bool
_bt_compare_array_scankey_args(IndexScanDesc scan, ScanKey arraysk, ScanKey skey,
							   FmgrInfo *orderproc, BTArrayKeyInfo *array,
							   bool *qual_ok)
```
## Detailed Description
This function implements scan key optimization by comparing array scan keys with scalar scan keys on the same index attribute. It eliminates array elements that are contradicted by scalar constraints, potentially making the scalar scan key redundant. For example, with a query "WHERE a IN (1, 2, 3) AND a < 2", it eliminates array elements 2 and 3, keeping only 1, and marks the "< 2" condition as redundant.

The function handles different comparison strategies (less than, equal, greater than, etc.) and can work with cross-type comparisons when the array and scalar values have different but compatible types. It uses binary search to efficiently locate matching elements in the sorted array and then applies the appropriate filtering logic based on the scalar scan key's strategy.

## Parameters / Member Variables
- `scan`: IndexScanDesc containing information about the index scan and relation
- `arraysk`: ScanKey representing the array scan condition (e.g., "IN" clause)
- `skey`: ScanKey representing the scalar scan condition (e.g., "<", "=", ">" clause)
- `*orderproc`: FmgrInfo structure containing the comparison procedure for ordering
- `*array`: BTArrayKeyInfo structure containing the array elements and metadata
- `*qual_ok`: Output parameter indicating whether the resulting qualification is satisfiable
## Dependencies
- Functions called/Symbols referenced:
  - [IndexScanDesc](../I/IndexScanDesc.md)
  - ScanKey
  - [BTArrayKeyInfo](../B/BTArrayKeyInfo.md)
  - SK_ISNULL, SK_ROW_HEADER, SK_ROW_MEMBER, SK_SEARCHARRAY
  - RegProcedure
  - [get_opfamily_proc](../g/get_opfamily_proc.md)
  - BTORDER_PROC
  - RegProcedureIsValid
  - [fmgr_info](../f/fmgr_info.md)
  - [_bt_binsrch_array_skey](_bt_binsrch_array_skey.md)
  - NoMovementScanDirection
  - BTLessStrategyNumber, BTLessEqualStrategyNumber
  - BTEqualStrategyNumber
  - BTGreaterEqualStrategyNumber, BTGreaterStrategyNumber
- Called from (representative examples):
  - [_bt_compare_scankey_args](_bt_compare_scankey_args.md)

## Notes and Other Information
- Returns true if comparison was successful, false if required comparison procedures are unavailable
- Modifies the array in-place to eliminate contradictory elements
- Sets *qual_ok to false when the qualification becomes unsatisfiable (no valid array elements remain)
- Handles cross-type comparisons by looking up appropriate ORDER procedures from the operator family
- Supports all B-tree strategy numbers for scalar comparisons
- Uses binary search for efficient element location in sorted arrays
- The function is part of PostgreSQL's scan key preprocessing optimization system
- This is a static function, accessible only within nbtutils.c

## Simplified Source

```c
static bool
_bt_compare_array_scankey_args(IndexScanDesc scan, ScanKey arraysk, ScanKey skey,
                              FmgrInfo *orderproc, BTArrayKeyInfo *array,
                              bool *qual_ok)
{
    Relation rel = scan->indexRelation;
    Oid opcintype = rel->rd_opcintype[arraysk->sk_attno - 1];
    int cmpresult = 0, cmpexact = 0, matchelem, new_nelems = 0;
    FmgrInfo crosstypeproc;
    FmgrInfo *orderprocp = orderproc;

    // Verify array and scalar scan keys are on same attribute
    Assert(arraysk->sk_attno == skey->sk_attno);
    Assert(array->num_elems > 0);

    // Check if we need cross-type comparison procedure
    if (skey->sk_subtype != opcintype && skey->sk_subtype != InvalidOid) {
        RegProcedure cmp_proc;
        Oid arraysk_elemtype = arraysk->sk_subtype;

        if (arraysk_elemtype == InvalidOid)
            arraysk_elemtype = rel->rd_opcintype[arraysk->sk_attno - 1];

        // Look up cross-type ORDER procedure
        cmp_proc = get_opfamily_proc(rel->rd_opfamily[arraysk->sk_attno - 1],
                                    skey->sk_subtype, arraysk_elemtype,
                                    BTORDER_PROC);
        if (!RegProcedureIsValid(cmp_proc)) {
            *qual_ok = false;
            return false;  // Can't make comparison
        }

        orderprocp = &crosstypeproc;
        fmgr_info(cmp_proc, orderprocp);
    }

    // Find best matching element in array using binary search
    matchelem = _bt_binsrch_array_skey(orderprocp, false,
                                      NoMovementScanDirection,
                                      skey->sk_argument, false, array,
                                      arraysk, &cmpresult);

    // Apply filtering based on scalar scan key strategy
    switch (skey->sk_strategy) {
        case BTLessStrategyNumber:
            cmpexact = 1;  // exclude exact match
            // FALL THRU
        case BTLessEqualStrategyNumber:
            if (cmpresult >= cmpexact)
                matchelem++;
            new_nelems = matchelem;  // Keep elements from start
            break;

        case BTEqualStrategyNumber:
            if (cmpresult != 0) {
                new_nelems = 0;  // Unsatisfiable qual
            } else {
                // Keep only matching element at start
                array->elem_values[0] = array->elem_values[matchelem];
                new_nelems = 1;
            }
            break;

        case BTGreaterEqualStrategyNumber:
            cmpexact = 1;  // include exact match
            // FALL THRU
        case BTGreaterStrategyNumber:
            if (cmpresult >= cmpexact)
                matchelem++;
            // Shift remaining elements to start
            new_nelems = array->num_elems - matchelem;
            memmove(array->elem_values, array->elem_values + matchelem,
                   sizeof(Datum) * new_nelems);
            break;

        default:
            elog(ERROR, "unrecognized StrategyNumber: %d", (int) skey->sk_strategy);
            break;
    }

    // Update array with filtered elements
    array->num_elems = new_nelems;
    *qual_ok = new_nelems > 0;

    return true;
}
```