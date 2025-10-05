# _bt_merge_arrays

## Location
[src/backend/access/nbtree/nbtutils.c:893-975](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtutils.c#L893-L975)

## Overview
Merges elements from two sorted arrays by finding their intersection, reorganizing the original array in-place to contain only elements that exist in both arrays.

## Definition

```c
static bool
_bt_merge_arrays(IndexScanDesc scan, ScanKey skey, FmgrInfo *sortproc,
				 bool reverse, Oid origelemtype, Oid nextelemtype,
				 Datum *elems_orig, int *nelems_orig,
				 Datum *elems_next, int nelems_next)
```
## Detailed Description
This function implements an intersection merge operation for two pre-sorted and deduplicated arrays. It's specifically designed for B-tree index preprocessing when encountering multiple array equality scan keys against the same index attribute. The function finds elements that exist in both arrays and stores them in the original array, effectively computing the intersection.

The function handles cross-type comparisons when the two arrays contain different but compatible element types by looking up the appropriate cross-type ORDER procedure from the operator family. If the required comparison procedure is not available, the function returns false, indicating that the arrays cannot be merged.

The merge operation uses a two-pointer approach to efficiently traverse both sorted arrays simultaneously, comparing elements and keeping only those that match.

## Parameters / Member Variables
- `scan`: IndexScanDesc containing information about the index scan and relation
- `skey`: ScanKey identifying the index column and providing collation information
- `*sortproc`: FmgrInfo structure containing the ORDER procedure used for sorting
- `reverse`: Boolean indicating the sort order direction
- `origelemtype`: OID of the element type in the original array
- `nextelemtype`: OID of the element type in the next array to merge
- `*elems_orig`: Original array to be modified in-place with merged results
- `*nelems_orig`: Pointer to the count of elements in original array (modified to reflect new count)
- `*elems_next`: Second array to merge with the original
- `nelems_next`: Number of elements in the second array
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

## Simplified Source

```c
static bool
_bt_merge_arrays(IndexScanDesc scan, ScanKey skey, FmgrInfo *sortproc,
                 bool reverse, Oid origelemtype, Oid nextelemtype,
                 Datum *elems_orig, int *nelems_orig,
                 Datum *elems_next, int nelems_next)
{
    BTScanOpaque so = (BTScanOpaque) scan->opaque;
    Relation rel = scan->indexRelation;
    BTSortArrayContext cxt;
    int nelems_orig_start = *nelems_orig;
    int nelems_orig_merged = 0;
    FmgrInfo *mergeproc = sortproc;
    FmgrInfo crosstypeproc;

    // Handle cross-type comparison if needed
    if (origelemtype != nextelemtype) {
        RegProcedure cmp_proc = get_opfamily_proc(rel->rd_opfamily[skey->sk_attno - 1],
                                                  origelemtype, nextelemtype, BTORDER_PROC);
        if (!RegProcedureIsValid(cmp_proc))
            return false;  // Can't merge - missing comparison

        mergeproc = &crosstypeproc;
        fmgr_info_cxt(cmp_proc, mergeproc, so->arrayContext);
    }

    // Set up comparison context
    cxt.sortproc = mergeproc;
    cxt.collation = skey->sk_collation;
    cxt.reverse = reverse;

    // Merge arrays by finding intersection
    for (int i = 0, j = 0; i < nelems_orig_start && j < nelems_next;) {
        Datum *oelem = elems_orig + i;
        Datum *nelem = elems_next + j;
        int res = _bt_compare_array_elements(oelem, nelem, &cxt);

        if (res == 0) {
            // Found matching element - keep it
            elems_orig[nelems_orig_merged++] = *oelem;
            i++;
            j++;
        } else if (res < 0) {
            i++;  // Original element is smaller - advance original
        } else {
            j++;  // Next element is smaller - advance next
        }
    }

    *nelems_orig = nelems_orig_merged;
    return true;
}
```