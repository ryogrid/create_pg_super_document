# range_gist_consistent_int_multirange

## Location
[src/backend/utils/adt/rangetypes_gist.c:977-1038](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes_gist.c#L977-L1038)

## Overview
Performs GiST consistent test on an index internal page when the query is a multirange type, determining whether to descend into subtrees during range index traversal.

## Definition

```c
static bool
range_gist_consistent_int_multirange(TypeCacheEntry *typcache,
									 StrategyNumber strategy,
									 const RangeType *key,
									 const MultirangeType *query)
```
## Detailed Description
This function implements the consistent test for GiST (Generalized Search Tree) index operations when searching with a multirange query against range index keys on internal nodes. The function evaluates various spatial relationships between a range key and a multirange query based on the specified strategy, returning whether it's necessary to descend into child nodes during index traversal.

The function handles all range strategy operators including before, overleft, overlaps, overright, after, adjacent, contains, contained_by, and equality. For internal nodes, the logic is designed to be conservative - it returns true when there's any possibility that qualifying tuples might exist in the subtree, ensuring that no valid results are missed during the search.

## Parameters / Member Variables
- `*typcache`: Type cache entry containing information about the range type being indexed
- `strategy`: Strategy number indicating the type of spatial relationship to test (e.g., RANGESTRAT_OVERLAPS, RANGESTRAT_CONTAINS)
- `*key`: The range value stored at this internal index node, representing the union of all ranges in the subtree
- `*query`: The multirange value being searched for in the index
## Dependencies
- Functions called/Symbols referenced:
  - RangeIsEmpty
  - MultirangeIsEmpty
  - RangeIsOrContainsEmpty
  - [range_overright_multirange_internal](range_overright_multirange_internal.md)
  - [range_after_multirange_internal](range_after_multirange_internal.md)
  - [range_overlaps_multirange_internal](range_overlaps_multirange_internal.md)
  - [range_before_multirange_internal](range_before_multirange_internal.md)
  - [range_overleft_multirange_internal](range_overleft_multirange_internal.md)
  - [range_adjacent_multirange_internal](range_adjacent_multirange_internal.md)
  - [range_contains_multirange_internal](range_contains_multirange_internal.md)
  - RANGESTRAT_* constants
- Called from (representative examples):
  - rangeCopy
  - [range_gist_consistent](range_gist_consistent.md)
  - [multirange_gist_consistent](../m/multirange_gist_consistent.md)

## Notes and Other Information
- This is a static function used internally within the range types GiST implementation
- The function implements conservative logic for internal nodes - [when](../w/when.md) in doubt, it returns true to ensure no valid results are missed
- Special handling is provided for empty ranges and multiranges, as they have unique containment semantics
- The RANGESTRAT_CONTAINED_BY case includes special logic for empty ranges, which are considered contained by any range
- The RANGESTRAT_EQ case handles empty query multiranges specially, only descending if the key contains empty ranges