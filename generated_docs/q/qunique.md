# qunique

## Location
src/include/lib/qunique.h: 21 - 45

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
  - compare (user-provided comparator function)
  - memcpy (for moving array elements)
- Called from (representative examples):
  - _bt_deadblocks
  - TidListEval
  - aclmembers
  - gtsvector_compress
  - tsq_mcontains
  - tsvector_delete_by_indices
  - array_to_tsvector
  - checkcondition_str
  - sort_snapshot
  - InitCatalogCache

## Notes and Other Information
- The function is implemented as a static inline function for performance optimization
- The input array must be pre-sorted using the same comparator function for correct operation
- Returns the new size of the array after duplicate removal
- The function modifies the array in-place, so the original array structure is altered
- For arrays with 0 or 1 elements, the function returns early without any processing
- This is a generic utility that works with any data type through void pointers and element width specification