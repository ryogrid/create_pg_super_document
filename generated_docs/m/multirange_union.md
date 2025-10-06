# multirange_union

## Location
[src/backend/utils/adt/multirangetypes.c:1082-1113](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes.c#L1082-L1113)

## Overview
Computes the union of two multirange values, combining all ranges from both inputs into a single multirange result.

## Definition
```c
Datum multirange_union(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the union operation for multirange types. It takes two multirange inputs and produces a new multirange containing all ranges from both inputs. The function includes optimizations for empty inputs: if either input is empty, it returns the other input directly. For non-empty inputs, it deserializes both multiranges, concatenates their range arrays, and constructs a new multirange. The make_multirange function handles the actual merging and normalization of overlapping ranges.

## Parameters / Member Variables
- `fcinfo`: PostgreSQL function call information structure containing two multirange arguments
  - Argument 0: First multirange operand
  - Argument 1: Second multirange operand

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_MULTIRANGE_P
  - MultirangeIsEmpty
  - PG_RETURN_MULTIRANGE_P
  - [multirange_get_typcache](multirange_get_typcache.md)
  - MultirangeTypeGetOid
  - [multirange_deserialize](multirange_deserialize.md)
  - [make_multirange](make_multirange.md)
  - [palloc0](../p/palloc0.md)
  - memcpy
- Called from (representative examples):
  - No direct references found (likely called through PostgreSQL's operator dispatch system)

## Notes and Other Information
- Implements the UNION operator for multirange types
- Optimizes for empty operands by returning the non-empty operand directly
- Handles range merging and normalization automatically through make_multirange
- Uses dynamic memory allocation to accommodate the combined range arrays
- The actual union logic (merging overlapping ranges) is handled by make_multirange
- Located in src/backend/utils/adt/multirangetypes.c:1082-1113

## Simplified Source

```c
Datum
multirange_union(PG_FUNCTION_ARGS)
{
    MultirangeType *mr1 = PG_GETARG_MULTIRANGE_P(0);
    MultirangeType *mr2 = PG_GETARG_MULTIRANGE_P(1);

    // Optimization: if either is empty, return the other
    if (MultirangeIsEmpty(mr1))
        PG_RETURN_MULTIRANGE_P(mr2);
    if (MultirangeIsEmpty(mr2))
        PG_RETURN_MULTIRANGE_P(mr1);

    TypeCacheEntry *typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr1));

    // Deserialize both multiranges into range arrays
    int32 range_count1, range_count2;
    RangeType **ranges1, **ranges2;
    multirange_deserialize(typcache->rngtype, mr1, &range_count1, &ranges1);
    multirange_deserialize(typcache->rngtype, mr2, &range_count2, &ranges2);

    // Combine all ranges into a single array
    int32 combined_count = range_count1 + range_count2;
    RangeType **combined_ranges = palloc0(combined_count * sizeof(RangeType *));
    memcpy(combined_ranges, ranges1, range_count1 * sizeof(RangeType *));
    memcpy(combined_ranges + range_count1, ranges2, range_count2 * sizeof(RangeType *));

    // Create new multirange (make_multirange handles merging overlapping ranges)
    PG_RETURN_MULTIRANGE_P(make_multirange(typcache->type_id, typcache->rngtype,
                                          combined_count, combined_ranges));
}
```

This function combines two multiranges by concatenating their ranges and letting `make_multirange` handle the union logic and normalization.