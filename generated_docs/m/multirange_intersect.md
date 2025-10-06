# multirange_intersect

## Location
[src/backend/utils/adt/multirangetypes.c:1230-1259](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes.c#L1230-L1259)

## Overview
Computes the intersection of two multirange values, returning a new multirange containing only the overlapping portions of the input multiranges.

## Definition

```c
Datum
multirange_intersect(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements the intersection operation for PostgreSQL multirange types. It takes two multirange arguments and returns their intersection as a new multirange. The function serves as a SQL-callable wrapper that handles PostgreSQL's function call interface and delegates the core intersection logic to .

The intersection operation finds all overlapping parts between the ranges in both multiranges. If either multirange is empty, the result is an empty multirange. The function handles type checking and ensures both multiranges are of the same type before performing the intersection.

## Parameters / Member Variables
- : Standard PostgreSQL function call interface containing:
  - Argument 0: First multirange ()
  - Argument 1: Second multirange ()

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_MULTIRANGE_P
  - MultirangeTypeGetOid
  - [multirange_get_typcache](multirange_get_typcache.md)
  - MultirangeIsEmpty
  - [make_empty_multirange](make_empty_multirange.md)
  - [multirange_deserialize](multirange_deserialize.md)
  - [multirange_intersect_internal](multirange_intersect_internal.md)
  - PG_RETURN_MULTIRANGE_P
- Called from:
  - No direct callers found (likely called through SQL function interface)

## Notes and Other Information
- This is a SQL-callable function that provides the intersection operator for multirange types
- Returns an empty multirange if either input is empty
- The actual intersection algorithm is implemented in 
- Part of PostgreSQL's range type system introduced for advanced range operations
- Located in src/backend/utils/adt/multirangetypes.c:1230-1259

## Simplified Source

```c
Datum
multirange_intersect(PG_FUNCTION_ARGS)
{
    MultirangeType *mr1 = PG_GETARG_MULTIRANGE_P(0);
    MultirangeType *mr2 = PG_GETARG_MULTIRANGE_P(1);

    Oid multirange_type_id = MultirangeTypeGetOid(mr1);
    TypeCacheEntry *typcache = multirange_get_typcache(fcinfo, multirange_type_id);
    TypeCacheEntry *range_type = typcache->rngtype;

    // Optimization: if either is empty, return empty multirange
    if (MultirangeIsEmpty(mr1) || MultirangeIsEmpty(mr2))
        PG_RETURN_MULTIRANGE_P(make_empty_multirange(multirange_type_id, range_type));

    // Deserialize both multiranges into range arrays
    int32 range_count1, range_count2;
    RangeType **ranges1, **ranges2;
    multirange_deserialize(range_type, mr1, &range_count1, &ranges1);
    multirange_deserialize(range_type, mr2, &range_count2, &ranges2);

    // Delegate to internal function for actual intersection logic
    PG_RETURN_MULTIRANGE_P(multirange_intersect_internal(multirange_type_id,
                                                        range_type,
                                                        range_count1, ranges1,
                                                        range_count2, ranges2));
}
```

This function computes the intersection of two multiranges, with optimization for empty inputs and delegation to the internal implementation.