# _bt_merge_arrays

## Location
[src/backend/access/nbtree/nbtutils.c:893-975](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtutils.c#L893-L975)

## Overview
Merges elements from two sorted arrays by finding their intersection, reorganizing the original array in-place to contain only elements that exist in both arrays.

## Definition


## Detailed Description
This function implements an intersection merge operation for two pre-sorted and deduplicated arrays. It's specifically designed for B-tree index preprocessing when encountering multiple array equality scan keys against the same index attribute. The function finds elements that exist in both arrays and stores them in the original array, effectively computing the intersection.

The function handles cross-type comparisons when the two arrays contain different but compatible element types by looking up the appropriate cross-type ORDER procedure from the operator family. If the required comparison procedure is not available, the function returns false, indicating that the arrays cannot be merged.

The merge operation uses a two-pointer approach to efficiently traverse both sorted arrays simultaneously, comparing elements and keeping only those that match.

## Parameters / Member Variables
- : IndexScanDesc containing information about the index scan and relation
- : ScanKey identifying the index column and providing collation information
- : FmgrInfo structure containing the ORDER procedure used for sorting
- : Boolean indicating the sort order direction
- : OID of the element type in the original array
- : OID of the element type in the next array to merge
- : Original array to be modified in-place with merged results
- : Pointer to the count of elements in original array (modified to reflect new count)
- : Second array to merge with the original
- : Number of elements in the second array

## Dependencies
- Functions called/Symbols referenced:
  - [IndexScanDesc](../I/IndexScanDesc.md)
  - ScanKey
  - BTScanOpaque
  - [BTSortArrayContext](../B/BTSortArrayContext.md)
  - RegProcedure
  - [get_opfamily_proc](../g/get_opfamily_proc.md)
  - BTORDER_PROC
  - RegProcedureIsValid
  - [fmgr_info_cxt](../f/fmgr_info_cxt.md)
  - [_bt_compare_array_elements](_bt_compare_array_elements.md)
- Called from (representative examples):
  - [_bt_preprocess_array_keys](_bt_preprocess_array_keys.md)

## Notes and Other Information
- Returns true if merge was successful, false if required comparison procedures are unavailable
- Both input arrays must be pre-sorted and deduplicated before calling this function
- Elements are never copied between arrays; only the original array is modified
- Handles cross-type comparisons when element types differ but are compatible
- Uses intersection semantics: only elements present in both arrays are retained
- The function optimizes scan key processing by eliminating redundant array conditions
- This is a static function, accessible only within nbtutils.c