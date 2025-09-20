# TuplesortIndexBTreeArg

## Location
[src/backend/utils/sort/tuplesortvariants.c:126-137](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplesortvariants.c#L126-L137)

## Overview
A data structure that extends TuplesortIndexArg with additional fields specific to B-tree index sorting, used to handle unique constraint enforcement during index creation.

## Definition

```c
structure pointed by "TuplesortPublic.arg" for the index_hash subcase.
 */
typedef struct
{
	TuplesortIndexArg index;

	uint32		high_mask;		/* masks for sortable part of hash code */
	uint32		low_mask;
	uint32		max_buckets;
} TuplesortIndexHashArg;
```
## Detailed Description
TuplesortIndexBTreeArg is a specialized data structure used by PostgreSQL's tuple sorting mechanism for B-tree index creation. It inherits the basic index sorting functionality from TuplesortIndexArg and adds specific fields to handle unique constraints. This structure is pointed to by TuplesortPublic.arg in the index_btree subcase and is used exclusively by IndexTuple routines during B-tree index construction.

The structure enables the sorting system to enforce uniqueness constraints during the index build process, allowing it to detect and handle duplicate entries according to the specified unique constraint behavior.

## Parameters / Member Variables
- : Base TuplesortIndexArg structure containing heapRel (table being indexed) and indexRel (index being built)
- : Boolean flag that determines whether the sorting process should complain (raise an error) when duplicate tuples are encountered
- : Boolean flag that controls unique constraint null treatment behavior, determining whether NULL values are considered distinct or not in unique constraints

## Dependencies
- Functions called/Symbols referenced:
  - TuplesortIndexArg (base structure)
- Called from (representative examples):
  - [tuplesort_begin_index_btree](../t/tuplesort_begin_index_btree.md) (src/backend/utils/sort/tuplesortvariants.c:364, 369)
  - [tuplesort_begin_index_gist](../t/tuplesort_begin_index_gist.md) (src/backend/utils/sort/tuplesortvariants.c:500, 504)
  - [comparetup_index_btree_tiebreak](../c/comparetup_index_btree_tiebreak.md) (src/backend/utils/sort/tuplesortvariants.c:1470)

## Notes and Other Information
- This structure is specific to the index_btree sorting subcase and is not used for other index types
- The structure is set by tuplesort_begin_index_xxx functions and used only by IndexTuple routines
- The uniqueNullsNotDistinct field supports PostgreSQL's NULLS [NOT] DISTINCT unique constraint options
- Part of PostgreSQL's tuple sorting variants system located in src/backend/utils/sort/tuplesortvariants.c