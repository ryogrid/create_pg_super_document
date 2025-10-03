# adjacent_cmp_bounds

## Location
[src/backend/utils/adt/rangetypes_spgist.c:785-886](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes_spgist.c#L785-L886)

## Overview
Determines the spatial relationship between an argument bound and centroid bound for adjacent range searches in SP-GiST indexing.

## Definition

```c
static int
adjacent_cmp_bounds(TypeCacheEntry *typcache, const RangeBound *arg,
					const RangeBound *centroid)
```
## Detailed Description
This static function is used during adjacent range searches in SP-GiST indexing to determine which side of a centroid partition should be searched. It analyzes the relationship between an argument bound (from the search query) and a centroid bound to determine if adjacent ranges would be found in the "left" or "right" partition.

The function handles two distinct cases:
1. **Upper bound search**: When the argument is an upper bound and we're searching for adjacent lower bounds that must be larger than the argument
2. **Lower bound search**: When the argument is a lower bound and we're searching for adjacent upper bounds that must be smaller than the argument

The logic incorporates adjacency checking using  to handle edge cases where bounds are exactly adjacent, which affects the search direction.

## Parameters / Member Variables
- `*typcache`: Type cache entry containing comparison functions for the range type
- `*arg`: The argument bound from the search query
- `*centroid`: The centroid bound used for partitioning
## Dependencies
- Functions called/Symbols referenced:
  - [range_cmp_bounds](../r/range_cmp_bounds.md)
  - [bounds_adjacent](../b/bounds_adjacent.md)
- Called from (representative examples):
  - [adjacent_inner_consistent](adjacent_inner_consistent.md)

## Notes and Other Information
- Returns -1 for "left" case (arg < centroid) and 1 for "right" case (arg >= centroid)
- The function assumes  (different bound types)
- Includes detailed table examples in comments showing how different argument/centroid combinations determine search direction
- Critical for proper adjacent range searching in quadrant-based SP-GiST partitioning
- The "left/right" terminology corresponds to "down/up" in spatial quadrant arrangement