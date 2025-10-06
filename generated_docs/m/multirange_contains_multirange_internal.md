# multirange_contains_multirange_internal

## Location
[src/backend/utils/adt/multirangetypes.c:2266-2327](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes.c#L2266-L2327)

## Overview
Internal function that implements the core logic to test whether one multirange contains every range from another multirange using an efficient O(n+m) algorithm.

## Definition
```c
bool multirange_contains_multirange_internal(TypeCacheEntry *rangetyp,
                                            const MultirangeType *mr1,
                                            const MultirangeType *mr2)
```

## Detailed Description
The `multirange_contains_multirange_internal` function implements the core containment logic for multirange types. It determines whether the first multirange (`mr1`) contains every range from the second multirange (`mr2`). The function uses an efficient two-pointer technique to walk through both multiranges in tandem, avoiding O(n²) complexity.

The algorithm follows these key principles:
- Empty multiranges follow the same containment rules as ranges
- An empty multirange contains another empty multirange
- An empty multirange cannot contain non-empty multiranges
- Any multirange contains an empty multirange

For non-empty multiranges, the function ensures every range in `mr2` is contained by some range in `mr1` by advancing through `mr1`'s ranges while testing containment.

## Parameters / Member Variables
- `rangetyp`: TypeCacheEntry pointer for the range type operations
- `mr1`: The potentially containing multirange (const MultirangeType *)
- `mr2`: The multirange to test for containment (const MultirangeType *)

## Dependencies
- Functions called/Symbols referenced:
  - [multirange_get_bounds](multirange_get_bounds.md) - Extract bounds from multirange ranges
  - [range_cmp_bounds](../r/range_cmp_bounds.md) - Compare range bounds
  - [range_bounds_contains](../r/range_bounds_contains.md) - Test if one range contains another
- Called from (representative examples):
  - [multirange_contains_multirange](multirange_contains_multirange.md) - Direct wrapper function
  - [multirange_contained_by_multirange](multirange_contained_by_multirange.md) - With reversed arguments
  - `PG_RETURN_MULTIRANGE_P` - Referenced in header files

## Notes and Other Information
- Uses an O(n+m) algorithm by walking through both multiranges simultaneously
- Handles empty multiranges according to standard range containment semantics
- Relies on multiranges being canonicalized (normalized and sorted)
- Comments note that non-canonicalized multiranges may produce unexpected results
- The algorithm discards ranges from mr1 while they are completely left of the current mr2 range
- Located in src/backend/utils/adt/multirangetypes.c:2266-2327
- This is the workhorse function that both @> and <@ operators ultimately call

## Simplified Source

```c
bool multirange_contains_multirange_internal(TypeCacheEntry *rangetyp,
                                            const MultirangeType *mr1,
                                            const MultirangeType *mr2) {
    int32 range_count1 = mr1->rangeCount;
    int32 range_count2 = mr2->rangeCount;

    // Handle empty multiranges: empty contains empty, non-empty contains empty
    if (range_count2 == 0)
        return true;
    if (range_count1 == 0)
        return false;

    // Walk through both multiranges in tandem to avoid O(n²) complexity
    int i1 = 0, i2;
    RangeBound lower1, upper1, lower2, upper2;

    multirange_get_bounds(rangetyp, mr1, i1, &lower1, &upper1);

    for (i2 = 0; i2 < range_count2; i2++) {
        multirange_get_bounds(rangetyp, mr2, i2, &lower2, &upper2);

        // Skip ranges in mr1 that are completely left of current mr2 range
        while (range_cmp_bounds(rangetyp, &upper1, &lower2) < 0) {
            if (++i1 >= range_count1)
                return false;  // No more ranges in mr1 to check
            multirange_get_bounds(rangetyp, mr1, i1, &lower1, &upper1);
        }

        // Check if current mr1 range contains current mr2 range
        if (!range_bounds_contains(rangetyp, &lower1, &upper1, &lower2, &upper2))
            return false;
    }

    return true;  // All ranges in mr2 are contained
}
```