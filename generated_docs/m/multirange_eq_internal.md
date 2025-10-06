# multirange_eq_internal

## Location
[src/backend/utils/adt/multirangetypes.c:1864-1900](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes.c#L1864-L1900)

## Overview
An internal function that tests whether two multiranges are equal by comparing their range counts and individual range bounds.

## Definition
```c
bool multirange_eq_internal(TypeCacheEntry *rangetyp,
                           const MultirangeType *mr1,
                           const MultirangeType *mr2)
```

## Detailed Description
This function implements equality comparison for multiranges by performing element-wise comparison of all ranges within the multiranges. It first validates that both multiranges are of the same type, then compares their range counts. If the counts match, it iterates through each range pair, extracting bounds and comparing them using range-specific comparison functions. The comparison is strict - both lower and upper bounds must match exactly for each corresponding range position. The function assumes multiranges are already in normalized form (sorted, non-overlapping, non-adjacent ranges).

## Parameters / Member Variables
- `rangetyp`: TypeCacheEntry pointer containing type-specific information for range operations
- `mr1`: const MultirangeType pointer to the first multirange to compare
- `mr2`: const MultirangeType pointer to the second multirange to compare

## Dependencies
- Functions called/Symbols referenced:
  - `MultirangeTypeGetOid` - Get the OID of multirange types for validation
  - [multirange_get_bounds](multirange_get_bounds.md) - Extract bounds from specific ranges within multiranges
  - [range_cmp_bounds](../r/range_cmp_bounds.md) - Compare individual range bounds for equality
  - `RangeBound` - Structure for representing range boundaries
  - `elog` - PostgreSQL error logging function
- Called from (representative examples):
  - [multirange_eq](multirange_eq.md) - Public SQL equality function wrapper
  - [multirange_ne_internal](multirange_ne_internal.md) - Negated equality for inequality operations

## Notes and Other Information
- Performs type checking to ensure both multiranges are of compatible types
- Uses O(n) comparison where n is the number of ranges in the multiranges
- Relies on the normalized property of multiranges for correctness
- Early termination optimization: returns false immediately on first mismatch
- Part of PostgreSQL's multirange comparison operator family
- The function assumes input multiranges are valid and properly constructed

## Simplified Source

```c
bool multirange_eq_internal(TypeCacheEntry *rangetyp,
                           const MultirangeType *mr1,
                           const MultirangeType *mr2)
{
    // Validate same multirange type
    if (MultirangeTypeGetOid(mr1) != MultirangeTypeGetOid(mr2))
        elog(ERROR, "multirange types do not match");

    // Quick check: different range counts mean not equal
    if (mr1->rangeCount != mr2->rangeCount)
        return false;

    // Compare each range pair element-wise
    for (int i = 0; i < mr1->rangeCount; i++)
    {
        RangeBound lower1, upper1, lower2, upper2;

        // Get bounds for corresponding ranges
        multirange_get_bounds(rangetyp, mr1, i, &lower1, &upper1);
        multirange_get_bounds(rangetyp, mr2, i, &lower2, &upper2);

        // Both lower and upper bounds must match exactly
        if (range_cmp_bounds(rangetyp, &lower1, &lower2) != 0 ||
            range_cmp_bounds(rangetyp, &upper1, &upper2) != 0)
            return false;
    }

    return true;
}
```