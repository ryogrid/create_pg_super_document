# range_after_internal

## Location
[src/backend/utils/adt/rangetypes.c:702-726](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes.c#L702-L726)

## Overview
The  function determines whether one range is strictly positioned after (to the right of) another range, implementing the core logic for range ordering comparisons in the opposite direction of .

## Definition

```c
bool
range_after_internal(TypeCacheEntry *typcache, const RangeType *r1, const RangeType *r2)
```
## Detailed Description
This function implements the internal logic for determining if one range is strictly after another range. A range r1 is considered "after" r2 if the lower bound of r1 is greater than the upper bound of r2, meaning there is no overlap and r1 is positioned entirely to the right of r2 on the value axis.

The function performs several checks similar to : it validates that both ranges are of the same type, deserializes the range bounds, handles empty ranges (which are neither before nor after any other range), and finally compares the lower bound of the first range with the upper bound of the second range.

## Parameters / Member Variables
- `*typcache`: Type cache entry containing type-specific information for range operations
- `*r1`: The first range to compare (potential "after" range)
- `*r2`: The second range to compare (potential "before" range)
## Dependencies
- Functions called/Symbols referenced:
  -  - Gets the OID of range types for type matching validation
  -  - Deserializes ranges into their constituent bounds
  -  - Compares range boundaries
  -  - Structure type for representing range boundaries
  -  - PostgreSQL error logging function
- Called from (representative examples):
  -  - Public wrapper function
  -  - GiST index consistency checking
  -  - GiST leaf consistency checking
  -  - SP-GiST quadtree consistency checking
  -  - [Range](../R/Range.md) strategy macro

## Notes and Other Information
- Empty ranges are treated as neither before nor after any other range, ensuring consistent behavior
- The function enforces type safety by checking that both ranges are of the same type
- Uses bound comparison logic where r1 is after r2 if lower1 > upper2
- This is the complement to  - they implement opposite directional checks
- Critical for range indexing strategies in both GiST and SP-GiST implementations
- Located in src/backend/utils/adt/rangetypes.c:702-726

## Simplified Source

```c
bool range_after_internal(TypeCacheEntry *typcache, const RangeType *r1, const RangeType *r2) {
    RangeBound lower1, upper1, lower2, upper2;
    bool empty1, empty2;

    // Verify both ranges have the same type
    if (RangeTypeGetOid(r1) != RangeTypeGetOid(r2))
        elog(ERROR, "range types do not match");

    // Extract bounds and empty status from both ranges
    range_deserialize(typcache, r1, &lower1, &upper1, &empty1);
    range_deserialize(typcache, r2, &lower2, &upper2, &empty2);

    // Empty ranges are neither before nor after any other range
    if (empty1 || empty2)
        return false;

    // r1 is after r2 if r1's lower bound > r2's upper bound
    return (range_cmp_bounds(typcache, &lower1, &upper2) > 0);
}
``` 