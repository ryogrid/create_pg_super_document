# range_contained_by_multirange

## Location
src/backend/utils/adt/multirangetypes.c: 1746 - 1757

## Overview
PostgreSQL function that tests whether a range is contained by (completely encompassed within) a multirange, implementing the <@ operator for range-multirange containment.

## Definition
```c
Datum range_contained_by_multirange(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the PostgreSQL SQL function that checks if a range is contained by a multirange. It serves as the entry point for the <@ (contained by) operator when the left operand is a range and the right operand is a multirange. The function provides the inverse relationship to the @> (contains) operator, testing the same logical condition from the opposite perspective. The function extracts the range and multirange arguments from the PostgreSQL function call context, obtains the appropriate type cache information, and delegates to the same internal containment logic used by multirange_contains_range, since "A contained by B" is logically equivalent to "B contains A".

The function follows the standard PostgreSQL function interface pattern, using PG_FUNCTION_ARGS for parameter passing and PG_RETURN_BOOL for the return value.

## Parameters / Member Variables
- Function uses PG_FUNCTION_ARGS macro which provides:
  - `arg 0`: The range to test for containment (extracted via PG_GETARG_RANGE_P)
  - `arg 1`: The multirange that should contain the range (extracted via PG_GETARG_MULTIRANGE_P)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_RANGE_P (argument extraction macro)
  - MultirangeType (struct type)
  - PG_GETARG_MULTIRANGE_P (argument extraction macro)
  - [multirange_get_typcache](../m/multirange_get_typcache.md) (type cache retrieval)
  - MultirangeTypeGetOid (OID extraction utility)
  - [multirange_contains_range_internal](../m/multirange_contains_range_internal.md) (core containment logic)
- Called from (representative examples):
  - No direct references found (called via PostgreSQL's function dispatch system)

## Notes and Other Information
This is a PostgreSQL-exported function that can be called from SQL as part of range and multirange operations. It's typically invoked through the <@ operator syntax in SQL queries where a range is tested for containment within a multirange. The function is registered in the PostgreSQL system catalogs and made available to the SQL parser and executor. Notably, this function reuses the same internal logic as multirange_contains_range by simply swapping the argument order, demonstrating the symmetric nature of containment relationships.