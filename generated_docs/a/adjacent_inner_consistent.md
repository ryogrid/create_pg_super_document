# adjacent_inner_consistent

## Location
[src/backend/utils/adt/rangetypes_spgist.c:887-916](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes_spgist.c#L887-L916)

## Overview
Enhanced version of  that considers previous level centroid information to improve search precision during adjacent range queries.

## Definition

```c
static int
adjacent_inner_consistent(TypeCacheEntry *typcache, const RangeBound *arg,
						  const RangeBound *centroid, const RangeBound *prev)
```
## Detailed Description
This function extends  functionality by incorporating information from the previous traversal level to make more informed decisions about search direction. It addresses situations where the search has already moved in a specific direction at a previous level, potentially ruling out matches in certain directions at the current level.

The function performs a two-step analysis:
1. **Previous level validation**: Compares the intended search direction (based on argument vs previous centroid) with the actual direction taken (previous vs current centroid)
2. **Consistency check**: If the intended and actual directions don't match, it indicates we're exploring a branch for the opposite bound's adjacency, and no matches exist in the current direction

This optimization helps avoid unnecessary tree traversals during adjacent range searches by leveraging information from the traversal path.

## Parameters / Member Variables
- `*typcache`: Type cache entry containing comparison functions for the range type
- `*arg`: The argument bound from the search query
- `*centroid`: The current level's centroid bound
- `*prev`: The previous level's centroid bound (can be NULL)
## Dependencies
- Functions called/Symbols referenced:
  - [adjacent_cmp_bounds](adjacent_cmp_bounds.md)
  - [range_cmp_bounds](../r/range_cmp_bounds.md)
- Called from (representative examples):
  - [spg_range_quad_inner_consistent](../s/spg_range_quad_inner_consistent.md)

## Notes and Other Information
- Returns -1 for left search, 1 for right search, 0 for no matches possible
- When  is NULL, falls back to standard  behavior
- Contains detailed comments explaining limitations where comparing only two levels isn't foolproof
- The function acknowledges it could be further optimized by explicitly tracking which bound is being searched rather than deducing from centroids
- Critical for reducing unnecessary traversals in multi-level adjacent range searches
- Part of the RANGESTRAT_ADJACENT strategy implementation in SP-GiST indexing