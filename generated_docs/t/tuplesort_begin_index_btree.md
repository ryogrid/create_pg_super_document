# tuplesort_begin_index_btree

## Location
src/backend/utils/sort/tuplesortvariants.c: 352 - 436

## Overview
Initializes a Tuplesortstate for sorting index tuples during B-tree index creation, with support for uniqueness enforcement and parallel index building operations.

## Definition


## Detailed Description
This function creates a specialized tuplesort state for B-tree index creation operations. It configures the sorting infrastructure to handle index tuples according to the index's key attributes, with specific support for uniqueness constraints. The function sets up comparison functions optimized for index tuples, prepares sort support data for each index key, and configures uniqueness enforcement when required. It respects the index's ordering properties including collation, null handling, and ascending/descending sort directions.

## Parameters / Member Variables
- : The heap relation being indexed
- : The B-tree index relation being created
- : Whether to enforce uniqueness constraints during sorting
- : Whether NULL values should be considered distinct for uniqueness
- : Amount of memory (in KB) available for sorting operations
- : Coordination structure for parallel sorting operations
- : Sorting options bitmask (e.g., TUPLESORT_RANDOMACCESS)

## Dependencies
- Functions called/Symbols referenced:
  - [tuplesort_begin_common](tuplesort_begin_common.md)
  - TuplesortstateGetPublic
  - IndexRelationGetNumberOfKeyAttributes
  - [_bt_mkscankey](../b/_bt_mkscankey.md)
  - [removeabbrev_index](../r/removeabbrev_index.md)
  - [comparetup_index_btree](../c/comparetup_index_btree.md)
  - [comparetup_index_btree_tiebreak](../c/comparetup_index_btree_tiebreak.md)
  - [writetup_index](../w/writetup_index.md)
  - [readtup_index](../r/readtup_index.md)
  - PrepareSortSupportFromIndexRel
- Called from (representative examples):
  - [_bt_spools_heapscan](../b/_bt_spools_heapscan.md) (nbtsort.c:428, nbtsort.c:469)
  - [_bt_parallel_scan_and_sort](../b/_bt_parallel_scan_and_sort.md) (nbtsort.c:1879, nbtsort.c:1905)

## Notes and Other Information
- Creates a TuplesortIndexBTreeArg structure to store index-specific parameters including heap and index relations
- Supports uniqueness enforcement with configurable handling of NULL values
- Uses index scan key information to configure sort support for proper ordering
- Enables datum1 optimization for improved performance with the first sort key
- Respects index-specific properties like DESC ordering and NULLS FIRST/LAST from scan keys
- Used primarily during CREATE INDEX operations and parallel index building
- The function handles both regular and parallel index creation scenarios through the coordinate parameter