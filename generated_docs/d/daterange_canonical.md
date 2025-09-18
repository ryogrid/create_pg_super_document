# daterange_canonical

## Location
[src/backend/utils/adt/rangetypes.c:1558-1620](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes.c#L1558-L1620)

## Overview
Converts a daterange to its canonical form by normalizing bounds to use consistent inclusivity/exclusivity conventions while handling date-specific validation.

## Definition


## Detailed Description
This function standardizes daterange values to a canonical representation where lower bounds are inclusive and upper bounds are exclusive. Similar to the integer range canonical functions but specifically designed for date ranges, it handles date-specific concerns such as finite/infinite dates and date validity checking. The function converts exclusive lower bounds to inclusive by incrementing the date value, and converts inclusive upper bounds to exclusive by incrementing the date value.

The function includes special handling for infinite dates using DATE_NOT_FINITE checks and validates date ranges using IS_VALID_DATE to prevent invalid date values after incrementing operations.

## Parameters / Member Variables
- : The input daterange value to be canonicalized (accessed via )

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_RANGE_P
  - [range_get_typcache](../r/range_get_typcache.md)
  - RangeTypeGetOid
  - [range_deserialize](../r/range_deserialize.md)
  - DATE_NOT_FINITE
  - DatumGetDateADT
  - IS_VALID_DATE
  - DateADTGetDatum
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