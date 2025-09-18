# range_contains_multirange

## Location
src/backend/utils/adt/multirangetypes.c: 1733 - 1745

## Overview
PostgreSQL function that tests whether a single range contains (completely encompasses) an entire multirange, implementing the @> operator for range-multirange containment.

## Definition
```c
Datum range_contains_multirange(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the PostgreSQL SQL function that checks if a range contains a multirange. It serves as the entry point for the @> (contains) operator when the left operand is a range and the right operand is a multirange. For containment to be true, the single range must completely encompass all constituent ranges within the multirange. The function extracts the range and multirange arguments from the PostgreSQL function call context, obtains the appropriate type cache information, and delegates the actual containment logic to the internal helper function.

The function follows the standard PostgreSQL function interface pattern, using PG_FUNCTION_ARGS for parameter passing and PG_RETURN_BOOL for the return value.

## Parameters / Member Variables
- Function uses PG_FUNCTION_ARGS macro which provides:
  - `arg 0`: The range to test (extracted via PG_GETARG_RANGE_P)
  - `arg 1`: The multirange to check for containment (extracted via PG_GETARG_MULTIRANGE_P)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_RANGE_P (argument extraction macro)
  - MultirangeType (struct type)
  - PG_GETARG_MULTIRANGE_P (argument extraction macro)
  - multirange_get_typcache (type cache retrieval)
  - MultirangeTypeGetOid (OID extraction utility)
  - range_contains_multirange_internal (core containment logic)
- Called from (representative examples):
  - No direct references found (called via PostgreSQL's function dispatch system)

## Notes and Other Information
This is a PostgreSQL-exported function that can be called from SQL as part of range and multirange operations. It's typically invoked through the @> operator syntax in SQL queries where a range is tested for containment of a multirange. The function is registered in the PostgreSQL system catalogs and made available to the SQL parser and executor. This represents the inverse relationship of multirange_contains_range, testing if a single contiguous range can contain all the potentially discontinuous ranges within a multirange.