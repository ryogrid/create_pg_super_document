# _bt_preprocess_array_keys

## Location
[src/backend/access/nbtree/nbtutils.c:269-550](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtutils.c#L269-L550)

## Overview
Preprocesses SK_SEARCHARRAY scan keys by deconstructing arrays and setting up BTArrayKeyInfo for each equality-type key, performing optimization by merging arrays and eliminating redundant elements.

## Definition

```c
struct the array into elements.  Anything allocated
		 * here (including a possibly detoasted array value) is in the
		 * workspace context.
		 */
		arrayval = DatumGetArrayTypeP(cur->sk_argument);
```
## Detailed Description
This function performs sophisticated preprocessing of array scan keys (SK_SEARCHARRAY) to optimize B-tree searches. It handles several key optimizations:

1. **Inequality Array Optimization**: For inequality operations (<, <=, >=, >), it finds the extreme element value and replaces the entire array with that scalar value, eliminating all redundant array elements.

2. **Array Merging**: When multiple equality array keys exist for the same index attribute, it merges them by finding intersecting elements, which can eliminate many redundant elements and detect contradictory conditions.

3. **Memory Management**: Creates a scan-lifespan memory context to hold array-associated data, which can be reset on rescans.

4. **Array Processing**: For each array key, it:
   - Deconstructs the array into individual elements
   - Removes null elements (assuming all btree operators are strict)
   - Sorts elements in index column order
   - Eliminates duplicates
   - Sets up comparison procedures for binary searches

The function returns a modified copy of the scan keys with array keys processed, while setting references in BTArrayKeyInfo to support later finalization.

## Parameters / Member Variables
- : The index scan descriptor containing the scan keys to be processed

## Dependencies
- Functions called/Symbols referenced:
  - BTScanOpaque
  - AllocSetContextCreate
  - [MemoryContextReset](../M/MemoryContextReset.md)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - DatumGetArrayTypeP
  - [get_typlenbyvalalign](../g/get_typlenbyvalalign.md)
  - [deconstruct_array](../d/deconstruct_array.md)
  - [_bt_find_extreme_element](_bt_find_extreme_element.md)
  - [_bt_setup_array_cmp](_bt_setup_array_cmp.md)
  - [_bt_sort_array_elements](_bt_sort_array_elements.md)
  - [_bt_merge_arrays](_bt_merge_arrays.md)
  - ARR_ELEMTYPE
  - BTLessStrategyNumber, BTEqualStrategyNumber, BTGreaterStrategyNumber
  - SK_SEARCHARRAY, SK_ISNULL
  - INDOPTION_DESC
  - InvalidStrategy

- Called from (representative examples):
  - [_bt_preprocess_keys](_bt_preprocess_keys.md) (main preprocessing entry point)

## Notes and Other Information
- Returns NULL if no array keys are present or if the scan qualification becomes unsatisfiable
- Handles cross-type equality operators by setting up separate ORDER procedures for sorting
- Array elements are sorted in the same ordering as the index column to enable lockstep advancement during scans
- Sets so->qual_ok to false when contradictory conditions are detected (e.g., no intersecting elements)
- The function creates a temporary copy of scan keys rather than modifying the original to support btrescan operations
- Eliminated array scan keys are marked with InvalidStrategy to signal the caller to ignore them
- Memory allocation occurs in the array context which persists for the scan lifetime but can be reset on rescans

## Simplified Source

```c
static ScanKey
_bt_preprocess_array_keys(IndexScanDesc scan)
{
    BTScanOpaque so = (BTScanOpaque) scan->opaque;
    Relation rel = scan->indexRelation;
    int numberOfKeys = scan->numberOfKeys;
    int numArrayKeys = 0;
    ScanKey arrayKeyData;

    // Quick check for array keys
    for (int i = 0; i < numberOfKeys; i++) {
        ScanKey cur = &scan->keyData[i];
        if (cur->sk_flags & SK_SEARCHARRAY) {
            numArrayKeys++;
            // Early exit if any array is null
            if (cur->sk_flags & SK_ISNULL) {
                so->qual_ok = false;
                return NULL;
            }
        }
    }

    // No array keys to process
    if (numArrayKeys == 0)
        return NULL;

    // Set up memory context for array processing
    if (so->arrayContext == NULL)
        so->arrayContext = AllocSetContextCreate(CurrentMemoryContext,
                                                  "BTree array context",
                                                  ALLOCSET_SMALL_SIZES);
    else
        MemoryContextReset(so->arrayContext);

    MemoryContext oldContext = MemoryContextSwitchTo(so->arrayContext);

    // Create modifiable copy of scan keys
    arrayKeyData = (ScanKey) palloc(numberOfKeys * sizeof(ScanKeyData));
    memcpy(arrayKeyData, scan->keyData, numberOfKeys * sizeof(ScanKeyData));

    // Allocate array structures
    so->arrayKeys = (BTArrayKeyInfo *) palloc(numArrayKeys * sizeof(BTArrayKeyInfo));
    so->orderProcs = (FmgrInfo *) palloc(numberOfKeys * sizeof(FmgrInfo));

    // Process each array key
    numArrayKeys = 0;
    int origarrayatt = InvalidAttrNumber;
    int origarraykey = -1;
    Oid origelemtype = InvalidOid;

    for (int i = 0; i < numberOfKeys; i++) {
        ScanKey cur = &arrayKeyData[i];
        if (!(cur->sk_flags & SK_SEARCHARRAY))
            continue;

        // Deconstruct array into elements
        ArrayType *arrayval = DatumGetArrayTypeP(cur->sk_argument);
        int16 elmlen;
        bool elmbyval;
        char elmalign;
        int num_elems;
        Datum *elem_values;
        bool *elem_nulls;

        get_typlenbyvalalign(ARR_ELEMTYPE(arrayval), &elmlen, &elmbyval, &elmalign);
        deconstruct_array(arrayval, ARR_ELEMTYPE(arrayval),
                         elmlen, elmbyval, elmalign,
                         &elem_values, &elem_nulls, &num_elems);

        // Remove null elements
        int num_nonnulls = 0;
        for (int j = 0; j < num_elems; j++) {
            if (!elem_nulls[j])
                elem_values[num_nonnulls++] = elem_values[j];
        }

        if (num_nonnulls == 0) {
            so->qual_ok = false;
            break;
        }

        // Handle inequality strategies by finding extreme element
        Oid elemtype = (cur->sk_subtype == InvalidOid) ?
                       rel->rd_opcintype[cur->sk_attno - 1] : cur->sk_subtype;

        switch (cur->sk_strategy) {
            case BTLessStrategyNumber:
            case BTLessEqualStrategyNumber:
                cur->sk_argument = _bt_find_extreme_element(scan, cur, elemtype,
                                                          BTGreaterStrategyNumber,
                                                          elem_values, num_nonnulls);
                continue;
            case BTGreaterEqualStrategyNumber:
            case BTGreaterStrategyNumber:
                cur->sk_argument = _bt_find_extreme_element(scan, cur, elemtype,
                                                          BTLessStrategyNumber,
                                                          elem_values, num_nonnulls);
                continue;
            case BTEqualStrategyNumber:
                break; // Continue processing
            default:
                elog(ERROR, "unrecognized StrategyNumber: %d", (int) cur->sk_strategy);
        }

        // Set up comparison procedures
        FmgrInfo sortproc;
        FmgrInfo *sortprocp = &sortproc;
        _bt_setup_array_cmp(scan, cur, elemtype, &so->orderProcs[i], &sortprocp);

        // Sort elements and remove duplicates
        bool reverse = (rel->rd_indoption[cur->sk_attno - 1] & INDOPTION_DESC) != 0;
        num_elems = _bt_sort_array_elements(cur, sortprocp, reverse,
                                           elem_values, num_nonnulls);

        // Handle array merging for same attribute
        if (origarrayatt == cur->sk_attno) {
            BTArrayKeyInfo *orig = &so->arrayKeys[origarraykey];
            if (_bt_merge_arrays(scan, cur, sortprocp, reverse,
                               origelemtype, elemtype,
                               orig->elem_values, &orig->num_elems,
                               elem_values, num_elems)) {
                pfree(elem_values);
                if (orig->num_elems == 0) {
                    so->qual_ok = false;
                    break;
                }
                cur->sk_strategy = InvalidStrategy; // Mark for elimination
                continue;
            }
        } else {
            // First array for this attribute
            origarrayatt = cur->sk_attno;
            origarraykey = numArrayKeys;
            origelemtype = elemtype;
        }

        // Set up BTArrayKeyInfo
        so->arrayKeys[numArrayKeys].scan_key = i;
        so->arrayKeys[numArrayKeys].num_elems = num_elems;
        so->arrayKeys[numArrayKeys].elem_values = elem_values;
        numArrayKeys++;
    }

    so->numArrayKeys = numArrayKeys;
    MemoryContextSwitchTo(oldContext);

    return arrayKeyData;
}
```