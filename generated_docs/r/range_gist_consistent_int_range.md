# range_gist_consistent_int_range

## Location
[src/backend/utils/adt/rangetypes_gist.c:915-976](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes_gist.c#L915-L976)

## Overview
A static function that implements GiST consistency testing for range queries on internal index pages, determining whether to descend into child nodes based on range relationship strategies.

## Definition

```c
static bool
range_gist_consistent_int_range(TypeCacheEntry *typcache,
								StrategyNumber strategy,
								const RangeType *key,
								const RangeType *query)
```
## Detailed Description
This function is a core component of the GiST range indexing system that determines whether an index internal page should be explored during a range query. It implements the consistency test for various range operators by evaluating the relationship between the index page's bounding range (key) and the query range.

The function handles all range strategies defined in the range operator class:

- **RANGESTRAT_BEFORE/AFTER**: Tests temporal/spatial ordering relationships
- **RANGESTRAT_OVERLEFT/OVERRIGHT**: Tests positional overlap relationships  
- **RANGESTRAT_OVERLAPS**: Tests for any overlap between ranges
- **RANGESTRAT_ADJACENT**: Tests for adjacency, including overlapping cases
- **RANGESTRAT_CONTAINS/CONTAINED_BY**: Tests containment relationships
- **RANGESTRAT_EQ**: Tests equality, with special empty range handling

The logic is designed to be conservative - it returns true when there's any possibility that descending into the subtree could yield matching results. Special attention is given to empty ranges, which have unique containment semantics that affect index traversal decisions.

## Parameters / Member Variables
- : TypeCacheEntry providing type information and comparison functions
- : StrategyNumber indicating which range operator is being evaluated
- : Index page's bounding range (represents all ranges in the subtree)
- : Query range being searched for
- Returns: boolean indicating whether to descend into this index page

## Dependencies
- Functions called/Symbols referenced:
  - : Check if a range is empty
  - : Check if range is empty or contains empty ranges
  - : Test if first range is over-right of second
  - : Test if first range is after second
  - : Test if ranges overlap
  - : Test if first range is before second
  - : Test if first range is over-left of second
  - : Test if ranges are adjacent
  - : Test if first range contains second
- Called from (representative examples):
  - : Main GiST consistency method for ranges
  - : GiST consistency method for multiranges

## Notes and Other Information
- Located in src/backend/utils/adt/rangetypes_gist.c:915-976
- Static function used internally within GiST range index operations
- Implements conservative logic to avoid false negatives in index traversal
- Handles empty range semantics correctly for each strategy type
- Critical for query performance - incorrect logic would cause missing results or unnecessary page reads
- The CONTAINED_BY and EQ strategies have special empty range handling due to PostgreSQL's range containment semantics

## Simplified Source

```c
static bool
range_gist_consistent_int_range(TypeCacheEntry *typcache,
                               StrategyNumber strategy,
                               const RangeType *key,
                               const RangeType *query)
{
    switch (strategy)
    {
        case RANGESTRAT_BEFORE:
            // Empty ranges can't have before relationship
            if (RangeIsEmpty(key) || RangeIsEmpty(query))
                return false;
            return (!range_overright_internal(typcache, key, query));

        case RANGESTRAT_OVERLEFT:
            if (RangeIsEmpty(key) || RangeIsEmpty(query))
                return false;
            return (!range_after_internal(typcache, key, query));

        case RANGESTRAT_OVERLAPS:
            return range_overlaps_internal(typcache, key, query);

        case RANGESTRAT_OVERRIGHT:
            if (RangeIsEmpty(key) || RangeIsEmpty(query))
                return false;
            return (!range_before_internal(typcache, key, query));

        case RANGESTRAT_AFTER:
            if (RangeIsEmpty(key) || RangeIsEmpty(query))
                return false;
            return (!range_overleft_internal(typcache, key, query));

        case RANGESTRAT_ADJACENT:
            if (RangeIsEmpty(key) || RangeIsEmpty(query))
                return false;
            // Adjacent includes both touching and overlapping ranges
            return range_adjacent_internal(typcache, key, query) ||
                   range_overlaps_internal(typcache, key, query);

        case RANGESTRAT_CONTAINS:
            return range_contains_internal(typcache, key, query);

        case RANGESTRAT_CONTAINED_BY:
            // Empty ranges are contained by anything
            if (RangeIsOrContainsEmpty(key))
                return true;
            return range_overlaps_internal(typcache, key, query);

        case RANGESTRAT_EQ:
            // Special handling for empty query ranges
            if (RangeIsEmpty(query))
                return RangeIsOrContainsEmpty(key);
            return range_contains_internal(typcache, key, query);

        default:
            elog(ERROR, "unrecognized range strategy: %d", strategy);
            return false;
    }
}
```