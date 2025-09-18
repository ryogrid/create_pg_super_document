# _bt_preprocess_array_keys

## Location
[src/backend/access/nbtree/nbtutils.c:269-550](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtutils.c#L269-L550)

## Overview
Preprocesses SK_SEARCHARRAY scan keys by deconstructing arrays and setting up BTArrayKeyInfo for each equality-type key, performing optimization by merging arrays and eliminating redundant elements.

## Definition


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