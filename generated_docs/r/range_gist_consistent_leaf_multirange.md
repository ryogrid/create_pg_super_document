# range_gist_consistent_leaf_multirange

## Location
[src/backend/utils/adt/rangetypes_gist.c:1093-1127](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes_gist.c#L1093-L1127)

## Overview
Performs GiST consistent test on an index leaf page with multirange query, determining whether a stored range matches the multirange query criteria using the specified spatial relationship strategy.

## Definition
```c
static bool range_gist_consistent_leaf_multirange(TypeCacheEntry *typcache,
                                                  StrategyNumber strategy,
                                                  const RangeType *key,
                                                  const MultirangeType *query)
```

## Detailed Description
This function implements the consistent test for GiST index operations on leaf pages when the stored data is a range type and the query is a multirange type. It provides exact matching semantics for range-to-multirange comparisons across all supported spatial relationship operators.

The function handles the asymmetric nature of range-to-multirange comparisons by delegating to specialized internal functions that understand the semantics of each relationship when applied between a single range and a collection of ranges. Notable cases include the RANGESTRAT_CONTAINED_BY strategy which reverses the operand order, and RANGESTRAT_EQ which uses a specialized equality function that checks if the range equals the union of the multirange.

## Parameters / Member Variables
- `typcache`: Type cache entry containing information about the range type being indexed
- `strategy`: Strategy number indicating the type of spatial relationship to test (e.g., RANGESTRAT_OVERLAPS, RANGESTRAT_CONTAINS)
- `key`: The range value stored in this leaf index entry
- `query`: The multirange value being searched for in the index

## Dependencies
- Functions called/Symbols referenced:
  - [range_before_multirange_internal](range_before_multirange_internal.md)
  - [range_overleft_multirange_internal](range_overleft_multirange_internal.md)
  - [range_overlaps_multirange_internal](range_overlaps_multirange_internal.md)
  - [range_overright_multirange_internal](range_overright_multirange_internal.md)
  - [range_after_multirange_internal](range_after_multirange_internal.md)
  - [range_adjacent_multirange_internal](range_adjacent_multirange_internal.md)
  - [range_contains_multirange_internal](range_contains_multirange_internal.md)
  - [multirange_contains_range_internal](../m/multirange_contains_range_internal.md)
  - [multirange_union_range_equal](../m/multirange_union_range_equal.md)
  - RANGESTRAT_* constants
  - elog (for error handling)
- Called from (representative examples):
  - rangeCopy
  - [range_gist_consistent](range_gist_consistent.md)
  - [multirange_gist_consistent](../m/multirange_gist_consistent.md)

## Notes and Other Information
- This is a static function used internally within the range types GiST implementation
- Provides exact matching for leaf-level operations, unlike the conservative approximations used for internal nodes
- The RANGESTRAT_CONTAINED_BY case uniquely reverses the operand order to call multirange_contains_range_internal
- The RANGESTRAT_EQ case uses multirange_union_range_equal to test if the range equals the multirange's union
- Supports all standard range spatial relationship operators adapted for range-to-multirange comparisons
- Each strategy delegates to specialized functions that handle the complexity of comparing single ranges against collections of ranges

## Simplified Source

```c
static bool
range_gist_consistent_leaf_multirange(TypeCacheEntry *typcache,
                                     StrategyNumber strategy,
                                     const RangeType *key,
                                     const MultirangeType *query)
{
    switch (strategy)
    {
        case RANGESTRAT_BEFORE:
            return range_before_multirange_internal(typcache, key, query);

        case RANGESTRAT_OVERLEFT:
            return range_overleft_multirange_internal(typcache, key, query);

        case RANGESTRAT_OVERLAPS:
            return range_overlaps_multirange_internal(typcache, key, query);

        case RANGESTRAT_OVERRIGHT:
            return range_overright_multirange_internal(typcache, key, query);

        case RANGESTRAT_AFTER:
            return range_after_multirange_internal(typcache, key, query);

        case RANGESTRAT_ADJACENT:
            return range_adjacent_multirange_internal(typcache, key, query);

        case RANGESTRAT_CONTAINS:
            return range_contains_multirange_internal(typcache, key, query);

        case RANGESTRAT_CONTAINED_BY:
            // Reverse operand order: check if multirange contains range
            return multirange_contains_range_internal(typcache, query, key);

        case RANGESTRAT_EQ:
            // Check if range equals the union of multirange
            return multirange_union_range_equal(typcache, key, query);

        default:
            elog(ERROR, "unrecognized range strategy: %d", strategy);
            return false;
    }
}
```