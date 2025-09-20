# range_cmp_bound_values

## Location
[src/backend/utils/adt/rangetypes.c:2090-2128](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes.c#L2090-L2128)

## Overview
Compares the values of two range boundary points, ignoring inclusive/exclusive flags and focusing only on the actual values and infinity semantics.

## Definition

```c
int
range_cmp_bound_values(TypeCacheEntry *typcache, const RangeBound *b1,
					   const RangeBound *b2)
```
## Detailed Description
The `range_cmp_bound_values` function provides a simplified comparison between two range boundary points that focuses purely on the values they contain, ignoring the inclusive/exclusive flags that are considered in `range_cmp_bounds`. This function is useful when you need to compare just the actual boundary values without the complex semantics of boundary inclusiveness. For infinite bounds, the lower/upper flag determines whether the infinity represents minus infinity (lower) or plus infinity (upper). For finite bounds, it performs a direct value comparison using the range type's comparison function.

## Parameters / Member Variables
- `typcache`: Type cache entry containing comparison function information for the range element type
- `b1`: First range boundary whose value will be compared
- `b2`: Second range boundary whose value will be compared

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetInt32](../D/DatumGetInt32.md)
  - [FunctionCall2Coll](../F/FunctionCall2Coll.md)
- Called from (representative examples):
  - [bounds_adjacent](../b/bounds_adjacent.md)
  - [range_serialize](range_serialize.md)

## Notes and Other Information
- This function is simpler than `range_cmp_bounds` as it ignores inclusive/exclusive semantics
- Infinite bounds are handled the same way: lower infinite = minus infinity, upper infinite = plus infinity
- The function returns the raw comparison result for finite values without any boundary-type adjustments
- Primarily used in internal range operations where only the actual values matter, not the boundary semantics
- Less commonly used than `range_cmp_bounds` but essential for specific range serialization and adjacency operations