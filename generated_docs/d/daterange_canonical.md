# daterange_canonical

## Location
[src/backend/utils/adt/rangetypes.c:1558-1620](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes.c#L1558-L1620)

## Overview
Converts a daterange to its canonical form by normalizing bounds to use consistent inclusivity/exclusivity conventions while handling date-specific validation.

## Definition

```c
Datum
daterange_canonical(PG_FUNCTION_ARGS)
```
## Detailed Description
This function standardizes daterange values to a canonical representation where lower bounds are inclusive and upper bounds are exclusive. Similar to the integer range canonical functions but specifically designed for date ranges, it handles date-specific concerns such as finite/infinite dates and date validity checking. The function converts exclusive lower bounds to inclusive by incrementing the date value, and converts inclusive upper bounds to exclusive by incrementing the date value.

The function includes special handling for infinite dates using DATE_NOT_FINITE checks and validates date ranges using IS_VALID_DATE to prevent invalid date values after incrementing operations.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: The input daterange value to be canonicalized (accessed via )
## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_RANGE_P
  - [range_get_typcache](../r/range_get_typcache.md)
  - RangeTypeGetOid
  - [range_deserialize](../r/range_deserialize.md)
  - DATE_NOT_FINITE
  - [DatumGetDateADT](../D/DatumGetDateADT.md)
  - IS_VALID_DATE
  - [DateADTGetDatum](../D/DateADTGetDatum.md)
  - [range_serialize](../r/range_serialize.md)
  - PG_RETURN_RANGE_P
  - ereturn (for error handling)
- Called from (representative examples):
  - No direct references found (likely called via function catalog for range operations)

## Notes and Other Information
- Part of the canonical functions for built-in range types, specifically for date ranges
- Handles date overflow by validating with IS_VALID_DATE after incrementing
- Special handling for infinite dates using DATE_NOT_FINITE checks
- Empty ranges are returned unchanged
- The canonical form uses inclusive lower bounds and exclusive upper bounds
- Error context is preserved for proper error reporting with DATETIME_VALUE_OUT_OF_RANGE errors
- This canonicalization is essential for consistent date range comparisons and operations
- Comments indicate that PG_INT32_MAX values are already eliminated before overflow checking

## Simplified Source

```c
Datum
daterange_canonical(PG_FUNCTION_ARGS)
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

    // Convert exclusive lower bound to inclusive (increment date)
    if (!lower.infinite && !DATE_NOT_FINITE(DatumGetDateADT(lower.val)) &&
        !lower.inclusive) {
        DateADT bnd = DatumGetDateADT(lower.val);
        bnd++;
        if (unlikely(!IS_VALID_DATE(bnd)))
            return (Datum) 0; // Date out of range error
        lower.val = DateADTGetDatum(bnd);
        lower.inclusive = true;
    }

    // Convert inclusive upper bound to exclusive (increment date)
    if (!upper.infinite && !DATE_NOT_FINITE(DatumGetDateADT(upper.val)) &&
        upper.inclusive) {
        DateADT bnd = DatumGetDateADT(upper.val);
        bnd++;
        if (unlikely(!IS_VALID_DATE(bnd)))
            return (Datum) 0; // Date out of range error
        upper.val = DateADTGetDatum(bnd);
        upper.inclusive = false;
    }

    // Return canonicalized range
    PG_RETURN_RANGE_P(range_serialize(typcache, &lower, &upper, false, escontext));
}
```