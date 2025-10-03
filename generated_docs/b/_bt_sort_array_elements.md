# _bt_sort_array_elements

## Location
[src/backend/access/nbtree/nbtutils.c:849-892](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtutils.c#L849-L892)

## Overview
Sorts array elements in-place and removes duplicates, returning the new count of unique elements after deduplication.

## Definition

```c
static int
_bt_sort_array_elements(ScanKey skey, FmgrInfo *sortproc, bool reverse,
						Datum *elems, int nelems)
```
## Detailed Description
This function performs in-place sorting of array elements using a provided comparison procedure from the index column's operator family. After sorting, it removes duplicate elements to create a unique sorted array. The sorting can be performed in either ascending or descending order based on the reverse parameter. The function uses PostgreSQL's qsort_arg and qunique_arg utility functions to perform the actual sorting and deduplication operations.

The function is optimized to handle the case where there are one or fewer elements (no sorting needed) and uses a context structure to pass comparison parameters to the sorting routines.

## Parameters / Member Variables
- `skey`: ScanKey containing the collation information for the index column
- `*sortproc`: FmgrInfo structure containing the comparison procedure to use for sorting
- `reverse`: Boolean flag indicating whether to sort in descending order (true) or ascending order (false)
- `*elems`: Array of Datum values to be sorted and deduplicated in-place
- `nelems`: Number of elements in the input array
## Dependencies
- Functions called/Symbols referenced:
  - [BTSortArrayContext](../B/BTSortArrayContext.md)
  - [_bt_compare_array_elements](_bt_compare_array_elements.md)
  - qsort_arg
  - [qunique_arg](../q/qunique_arg.md)
  - ScanKey
- Called from (representative examples):
  - [_bt_preprocess_array_keys](_bt_preprocess_array_keys.md)

## Notes and Other Information
- Returns the new number of elements after duplicate removal
- Modifies the input array in-place for memory efficiency
- Short-circuits for arrays with 0 or 1 elements since no sorting is needed
- Uses BTSortArrayContext to pass sorting parameters to the comparison function
- The comparison function _bt_compare_array_elements is used for both sorting and deduplication
- This is a static function, accessible only within nbtutils.c