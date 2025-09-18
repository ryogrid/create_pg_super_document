# _bt_sort_array_elements

## Location
src/backend/access/nbtree/nbtutils.c: 849 - 892

## Overview
Sorts array elements in-place and removes duplicates, returning the new count of unique elements after deduplication.

## Definition


## Detailed Description
This function performs in-place sorting of array elements using a provided comparison procedure from the index column's operator family. After sorting, it removes duplicate elements to create a unique sorted array. The sorting can be performed in either ascending or descending order based on the reverse parameter. The function uses PostgreSQL's qsort_arg and qunique_arg utility functions to perform the actual sorting and deduplication operations.

The function is optimized to handle the case where there are one or fewer elements (no sorting needed) and uses a context structure to pass comparison parameters to the sorting routines.

## Parameters / Member Variables
- : ScanKey containing the collation information for the index column
- : FmgrInfo structure containing the comparison procedure to use for sorting
- : Boolean flag indicating whether to sort in descending order (true) or ascending order (false)
- : Array of Datum values to be sorted and deduplicated in-place
- : Number of elements in the input array

## Dependencies
- Functions called/Symbols referenced:
  - BTSortArrayContext
  - _bt_compare_array_elements
  - qsort_arg
  - qunique_arg
  - ScanKey
- Called from (representative examples):
  - _bt_preprocess_array_keys

## Notes and Other Information
- Returns the new number of elements after duplicate removal
- Modifies the input array in-place for memory efficiency
- Short-circuits for arrays with 0 or 1 elements since no sorting is needed
- Uses BTSortArrayContext to pass sorting parameters to the comparison function
- The comparison function _bt_compare_array_elements is used for both sorting and deduplication
- This is a static function, accessible only within nbtutils.c