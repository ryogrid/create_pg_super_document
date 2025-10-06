# spg_range_quad_leaf_consistent

## Location
[src/backend/utils/adt/rangetypes_spgist.c:917-998](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes_spgist.c#L917-L998)

## Overview
SP-GiST leaf node consistent function that performs final range comparisons between indexed range values and query conditions.

## Definition

```c
Datum
spg_range_quad_leaf_consistent(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements the leaf node consistent logic for SP-GiST indexing of PostgreSQL range types. It represents the final step in index traversal where actual range values stored in leaf nodes are compared against query conditions to determine if they satisfy the search criteria.

The function iterates through all scan keys (query conditions) and applies the appropriate range comparison function based on the strategy:

- **RANGESTRAT_BEFORE**: Checks if leaf range is entirely before query range
- **RANGESTRAT_OVERLEFT**: Checks if leaf range overlaps or is left of query range  
- **RANGESTRAT_OVERLAPS**: Checks if leaf range overlaps with query range
- **RANGESTRAT_OVERRIGHT**: Checks if leaf range overlaps or is right of query range
- **RANGESTRAT_AFTER**: Checks if leaf range is entirely after query range
- **RANGESTRAT_ADJACENT**: Checks if leaf range is adjacent to query range
- **RANGESTRAT_CONTAINS**: Checks if leaf range contains query range
- **RANGESTRAT_CONTAINED_BY**: Checks if leaf range is contained by query range
- **RANGESTRAT_CONTAINS_ELEM**: Checks if leaf range contains query element
- **RANGESTRAT_EQ**: Checks if leaf range equals query range

All comparisons are exact (no recheck required), and the function returns true only if all query conditions are satisfied.

## Parameters / Member Variables
- : Input structure containing the leaf datum and scan keys
- : Output structure for returning results and recheck flag

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetRangeTypeP](../D/DatumGetRangeTypeP.md)
  - [range_get_typcache](../r/range_get_typcache.md)
  - RangeTypeGetOid
  - [range_before_internal](../r/range_before_internal.md)
  - [range_overleft_internal](../r/range_overleft_internal.md)
  - [range_overlaps_internal](../r/range_overlaps_internal.md)
  - [range_overright_internal](../r/range_overright_internal.md)
  - [range_after_internal](../r/range_after_internal.md)
  - [range_adjacent_internal](../r/range_adjacent_internal.md)
  - [range_contains_internal](../r/range_contains_internal.md)
  - [range_contained_by_internal](../r/range_contained_by_internal.md)
  - [range_contains_elem_internal](../r/range_contains_elem_internal.md)
  - [range_eq_internal](../r/range_eq_internal.md)
- Called from (representative examples):
  - No direct references found (likely called through function pointer in SP-GiST operator class)

## Notes and Other Information
- Sets  since all tests are exact and don't require revalidation
- Returns the original leaf datum as  for result retrieval
- Short-circuits on first non-matching condition to avoid unnecessary comparisons
- Part of the complete SP-GiST range indexing implementation alongside inner node functions
- Critical for the final filtering step in range-based index searches

## Simplified Source

```c
Datum spg_range_quad_leaf_consistent(PG_FUNCTION_ARGS)
{
    spgLeafConsistentIn *in = (spgLeafConsistentIn *) PG_GETARG_POINTER(0);
    spgLeafConsistentOut *out = (spgLeafConsistentOut *) PG_GETARG_POINTER(1);

    RangeType *leafRange = DatumGetRangeTypeP(in->leafDatum);
    TypeCacheEntry *typcache = range_get_typcache(fcinfo, RangeTypeGetOid(leafRange));

    out->recheck = false;      // All tests are exact
    out->leafValue = in->leafDatum;

    // Test leaf range against all scan key conditions
    for (int i = 0; i < in->nkeys; i++) {
        Datum keyDatum = in->scankeys[i].sk_argument;
        bool result;

        // Apply the appropriate range comparison based on strategy
        switch (in->scankeys[i].sk_strategy) {
            case RANGESTRAT_BEFORE:
                result = range_before_internal(typcache, leafRange, DatumGetRangeTypeP(keyDatum));
                break;
            case RANGESTRAT_OVERLEFT:
                result = range_overleft_internal(typcache, leafRange, DatumGetRangeTypeP(keyDatum));
                break;
            case RANGESTRAT_OVERLAPS:
                result = range_overlaps_internal(typcache, leafRange, DatumGetRangeTypeP(keyDatum));
                break;
            case RANGESTRAT_OVERRIGHT:
                result = range_overright_internal(typcache, leafRange, DatumGetRangeTypeP(keyDatum));
                break;
            case RANGESTRAT_AFTER:
                result = range_after_internal(typcache, leafRange, DatumGetRangeTypeP(keyDatum));
                break;
            case RANGESTRAT_ADJACENT:
                result = range_adjacent_internal(typcache, leafRange, DatumGetRangeTypeP(keyDatum));
                break;
            case RANGESTRAT_CONTAINS:
                result = range_contains_internal(typcache, leafRange, DatumGetRangeTypeP(keyDatum));
                break;
            case RANGESTRAT_CONTAINED_BY:
                result = range_contained_by_internal(typcache, leafRange, DatumGetRangeTypeP(keyDatum));
                break;
            case RANGESTRAT_CONTAINS_ELEM:
                result = range_contains_elem_internal(typcache, leafRange, keyDatum);
                break;
            case RANGESTRAT_EQ:
                result = range_eq_internal(typcache, leafRange, DatumGetRangeTypeP(keyDatum));
                break;
            default:
                elog(ERROR, "unrecognized range strategy: %d", in->scankeys[i].sk_strategy);
                break;
        }

        // Short-circuit if any condition fails
        if (!result)
            PG_RETURN_BOOL(false);
    }

    PG_RETURN_BOOL(true);
}
```