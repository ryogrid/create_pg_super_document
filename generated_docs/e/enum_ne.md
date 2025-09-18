# enum_ne

## Location
src/backend/utils/adt/enum.c: 333 - 341

## Overview
PostgreSQL built-in function that implements the inequality comparison operator (<> or !=) for enum data types using direct OID comparison.

## Definition


## Detailed Description
This function provides the inequality (not equal) comparison functionality for PostgreSQL enum types. Like enum_eq, enum_ne implements a highly optimized approach by performing direct OID inequality comparison without consulting enum metadata or using enum_cmp_internal().

Since enum values are stored as unique OIDs within each enum type, two enum values are not equal if and only if their OIDs are different. This makes the inequality check extremely fast and simple, complementing the enum_eq function.

The function follows PostgreSQL's standard function calling convention using PG_FUNCTION_ARGS and returns a boolean result wrapped in a Datum.

## Parameters / Member Variables
- Uses PG_FUNCTION_ARGS macro to access function arguments:
  - First argument (index 0): Left-hand side enum OID
  - Second argument (index 1): Right-hand side enum OID

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_OID (argument extraction macro)
  - PG_RETURN_BOOL (result return macro)
- Called from:
  - SQL queries using <> or != operator with enum types
  - System catalog functions
  - Query optimizer and executor

## Notes and Other Information
- Part of PostgreSQL's operator implementation framework for enum types
- Registered in the system catalogs as the implementation for the <> and != operators on enum types
- Significantly more efficient than other comparison operators due to direct OID comparison
- Does not need to access type cache or enum metadata unlike ordered comparison functions
- Forms an optimized pair with enum_eq for equality/inequality testing
- Works correctly because each enum value has a unique OID within its type
- Logical complement of enum_eq function