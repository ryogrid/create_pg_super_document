# int4range_canonical

## Location
[src/backend/utils/adt/rangetypes.c:1464-1510](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes.c#L1464-L1510)

## Overview
Converts an int4range (32-bit integer range) to its canonical form by normalizing bounds to use consistent inclusivity/exclusivity conventions.

## Definition

```c
Datum
int4range_canonical(PG_FUNCTION_ARGS)
```
## Detailed Description
This function standardizes int4range values to a canonical representation where lower bounds are inclusive and upper bounds are exclusive. For discrete types like integers, this canonical form provides a unique representation for equivalent ranges. The function converts exclusive lower bounds to inclusive by incrementing the value, and converts inclusive upper bounds to exclusive by incrementing the value. It includes overflow checking to prevent integer overflow when incrementing boundary values.

The canonical form ensures that ranges like [1,5) and (0,5) are represented consistently, which is important for range operations like equality comparisons and indexing.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: The input int4range value to be canonicalized (accessed via )
## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_RANGE_P
  - [range_get_typcache](../r/range_get_typcache.md)
  - RangeTypeGetOid
  - [range_deserialize](../r/range_deserialize.md)
  - [DatumGetInt32](../D/DatumGetInt32.md)
  - [Int32GetDatum](../I/Int32GetDatum.md)
  - [range_serialize](../r/range_serialize.md)
  - PG_RETURN_RANGE_P
  - ereturn (for error handling)
- Called from (representative examples):
  - No direct references found (likely called via function catalog for range operations)

## Notes and Other Information
- Part of the canonical functions for built-in range types
- Handles integer overflow by checking for PG_INT32_MAX before incrementing
- Empty ranges are returned unchanged
- The canonical form uses inclusive lower bounds and exclusive upper bounds
- Error context is preserved for proper error reporting in nested function calls
- This canonicalization is essential for consistent range comparisons and hash operations

## Simplified Source

```c
Datum
int4range_canonical(PG_FUNCTION_ARGS)
{
    RangeType *r = PG_GETARG_RANGE_P(0);
    RangeBound lower, upper;
    bool empty;

    // Get type cache and deserialize range
    TypeCacheEntry *typcache = range_get_typcache(fcinfo, RangeTypeGetOid(r));
    range_deserialize(typcache, r, &lower, &upper, &empty);

    // Return empty ranges unchanged
    if (empty)
        PG_RETURN_RANGE_P(r);

    // Convert exclusive lower bound to inclusive (increment value)
    if (!lower.infinite && !lower.inclusive) {
        int32 bnd = DatumGetInt32(lower.val);
        if (unlikely(bnd == PG_INT32_MAX))
            return (Datum) 0; // Overflow error
        lower.val = Int32GetDatum(bnd + 1);
        lower.inclusive = true;
    }

    // Convert inclusive upper bound to exclusive (increment value)
    if (!upper.infinite && upper.inclusive) {
        int32 bnd = DatumGetInt32(upper.val);
        if (unlikely(bnd == PG_INT32_MAX))
            return (Datum) 0; // Overflow error
        upper.val = Int32GetDatum(bnd + 1);
        upper.inclusive = false;
    }

    // Return canonicalized range
    PG_RETURN_RANGE_P(range_serialize(typcache, &lower, &upper, false, escontext));
}
```