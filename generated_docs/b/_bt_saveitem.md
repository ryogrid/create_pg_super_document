# _bt_saveitem

## Location
src/backend/access/nbtree/nbtsearch.c: 1945 - 1974

## Overview
Saves a non-pivot, non-posting index tuple into the current scan position's item array for B-tree scanning operations.

## Definition


## Detailed Description
This function is a helper routine used during B-tree page scanning to store index tuples in the scan state's current position structure. It specifically handles regular index tuples (not pivot tuples or posting tuples) by copying the heap TID, storing the page offset, and optionally copying the entire tuple data if tuple caching is enabled. The function ensures proper memory alignment when storing tuple data and maintains the scan state's tuple offset counter.

## Parameters / Member Variables
- : B-tree scan opaque structure containing the current scan state
- : Index position in the items array where this tuple should be stored
- : Offset number of the tuple on the current page
- : The index tuple to be saved (must not be a pivot or posting tuple)

## Dependencies
- Functions called/Symbols referenced:
  - BTreeTupleIsPivot (assertion check)
  - BTreeTupleIsPosting (assertion check)
  - IndexTupleSize (for tuple size calculation)
- Called from (representative examples):
  - _bt_readpage (multiple calls during page scanning)

## Notes and Other Information
- This function includes assertions to ensure the tuple is neither a pivot tuple nor a posting tuple, as these have different handling requirements
- Tuple copying is conditional based on whether  is allocated, allowing for memory-efficient scanning when full tuple data isn't needed
- Uses MAXALIGN to ensure proper memory alignment of stored tuples
- Part of the B-tree scanning infrastructure in nbtsearch.c