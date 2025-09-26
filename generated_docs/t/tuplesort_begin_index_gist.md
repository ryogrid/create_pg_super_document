# tuplesort_begin_index_gist

## Location
[src/backend/utils/sort/tuplesortvariants.c:490-554](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplesortvariants.c#L490-L554)

## Overview
Initializes a Tuplesortstate for sorting GiST (Generalized Search Tree) index tuples during index creation, with specialized support for GiST-specific sort operations.

## Definition

```c
Tuplesortstate *
tuplesort_begin_index_gist(Relation heapRel,
						   Relation indexRel,
						   int workMem,
						   SortCoordinate coordinate,
						   int sortopt)
```
## Detailed Description
This function creates a specialized tuplesort state for GiST index creation operations. It configures the sorting infrastructure for GiST index tuples, which have different requirements than B-tree indexes. The function sets up sort support for each index key attribute using GiST-specific preparation routines, handles collation information directly from the index relation, and disables uniqueness enforcement since GiST indexes don't support unique constraints. It reuses B-tree comparison functions but adapts them for GiST-specific sorting needs.

## Parameters / Member Variables
- : The heap relation being indexed
- : The GiST index relation being created
- : Amount of memory (in KB) available for sorting operations
- : Coordination structure for parallel sorting operations
- : Sorting options bitmask (e.g., TUPLESORT_RANDOMACCESS)

## Dependencies
- Functions called/Symbols referenced:
  - [tuplesort_begin_common](tuplesort_begin_common.md)
  - TuplesortstateGetPublic
  - IndexRelationGetNumberOfKeyAttributes
  - [removeabbrev_index](../r/removeabbrev_index.md)
  - [comparetup_index_btree](../c/comparetup_index_btree.md)
  - [comparetup_index_btree_tiebreak](../c/comparetup_index_btree_tiebreak.md)
  - [writetup_index](../w/writetup_index.md)
  - [readtup_index](../r/readtup_index.md)
  - [PrepareSortSupportFromGistIndexRel](../P/PrepareSortSupportFromGistIndexRel.md)
- Called from (representative examples):
  - [gistbuild](../g/gistbuild.md) (gistbuild.c:267)

## Notes and Other Information
- Reuses TuplesortIndexBTreeArg structure but sets enforceUnique and uniqueNullsNotDistinct to false since GiST doesn't support unique constraints
- Uses B-tree comparison functions (comparetup_index_btree) which are suitable for GiST index tuple sorting
- Sets nulls_first to false for all sort keys, following GiST conventions
- Uses PrepareSortSupportFromGistIndexRel for GiST-specific sort support preparation
- Derives collation information directly from indexRel->rd_indcollation array
- Enables datum1 optimization for improved performance with the first sort key
- The function sets ssup_attno to i+1 to avoid zero-based indexing issues
- Used during CREATE INDEX operations for GiST indexes to enable efficient bulk loading