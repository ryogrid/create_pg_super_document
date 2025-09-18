# qunique

## Location
[src/include/lib/qunique.h:21-45](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/qunique.h#L21-L45)

## Overview
A utility function that removes duplicates from a pre-sorted array using a user-supplied comparator function, typically used with arrays that have been sorted with qsort().

## Definition


## Detailed Description
The qunique function is designed to eliminate duplicate elements from an array that has already been sorted. It works by iterating through the array and comparing adjacent elements using the provided comparator function. When a duplicate is found (comparator returns 0), the element is skipped. When a unique element is found, it is moved to the next available position in the array. This in-place operation maintains the sorted order while removing duplicates.

The function uses a two-pointer technique where 'i' scans through all elements and 'j' tracks the position where the next unique element should be placed. The algorithm ensures that all unique elements are packed at the beginning of the array, and returns the new size of the deduplicated array.

## Parameters / Member Variables
- : Pointer to the array to be processed for duplicate removal
- : Number of elements in the input array
- : Size in bytes of each array element
- : Function pointer to comparator that returns 0 for equal elements, non-zero for different elements

## Dependencies
- Functions called/Symbols referenced:
  - [compare](../c/compare.md) (user-provided comparator function)
  - memcpy (for moving array elements)
- Called from (representative examples):
  - [_bt_deadblocks](../b/_bt_deadblocks.md)
  - TidListEval
  - [aclmembers](../a/aclmembers.md)
  - [gtsvector_compress](../g/gtsvector_compress.md)
  - [tsq_mcontains](../t/tsq_mcontains.md)
  - [tsvector_delete_by_indices](../t/tsvector_delete_by_indices.md)
  - [array_to_tsvector](../a/array_to_tsvector.md)
  - [checkcondition_str](../c/checkcondition_str.md)
  - [sort_snapshot](../s/sort_snapshot.md)
  - [InitCatalogCache](../I/InitCatalogCache.md)

## Notes and Other Information
- The function is implemented as a static inline function for performance optimization
- The input array must be pre-sorted using the same comparator function for correct operation
- Returns the new size of the array after duplicate removal
- The function modifies the array in-place, so the original array structure is altered
- For arrays with 0 or 1 elements, the function returns early without any processing
- This is a generic utility that works with any data type through void pointers and element width specification