# multirange_contains_range

## Location
src/backend/utils/adt/multirangetypes.c: 1721 - 1732

## Overview
PostgreSQL function that tests whether a multirange contains (completely encompasses) a given range, implementing the @> operator for multirange-range containment.

## Definition
```c
Datum multirange_contains_range(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the PostgreSQL SQL function that checks if a multirange contains a range. It serves as the entry point for the @> (contains) operator when the left operand is a multirange and the right operand is a range. The function extracts the multirange and range arguments from the PostgreSQL function call context, obtains the appropriate type cache information, and delegates the actual containment logic to the internal helper function.

The function follows the standard PostgreSQL function interface pattern, using PG_FUNCTION_ARGS for parameter passing and PG_RETURN_BOOL for the return value.

## Parameters / Member Variables
- Function uses PG_FUNCTION_ARGS macro which provides:
  - `arg 0`: The multirange to test (extracted via PG_GETARG_MULTIRANGE_P)
  - `arg 1`: The range to check for containment (extracted via PG_GETARG_RANGE_P)

## Dependencies
- Functions called/Symbols referenced:
  - MultirangeType (struct type)
  - PG_GETARG_MULTIRANGE_P (argument extraction macro)
  - PG_GETARG_RANGE_P (argument extraction macro)
  - [multirange_get_typcache](multirange_get_typcache.md) (type cache retrieval)
  - MultirangeTypeGetOid (OID extraction utility)
  - [multirange_contains_range_internal](multirange_contains_range_internal.md) (core containment logic)
- Called from (representative examples):
  - No direct references found (called via PostgreSQL's function dispatch system)

## Notes and Other Information
This is a PostgreSQL-exported function that can be called from SQL as part of multirange operations. It's typically invoked through the @> operator syntax in SQL queries. The function is registered in the PostgreSQL system catalogs and made available to the SQL parser and executor. The actual containment logic is implemented in the internal helper function to separate the PostgreSQL interface concerns from the core algorithm.