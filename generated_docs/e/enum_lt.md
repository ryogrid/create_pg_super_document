# enum_lt

## Location
src/backend/utils/adt/enum.c: 306 - 314

## Overview
PostgreSQL built-in function that implements the less-than comparison operator (<) for enum data types.

## Definition


## Detailed Description
This function provides the less-than comparison functionality for PostgreSQL enum types. It serves as a thin wrapper around enum_cmp_internal(), extracting the two enum OID arguments from the function call context and returning true if the first argument is less than the second according to the enum's defined ordering.

The function follows PostgreSQL's standard function calling convention using PG_FUNCTION_ARGS and returns a boolean result wrapped in a Datum.

## Parameters / Member Variables
- Uses PG_FUNCTION_ARGS macro to access function arguments:
  - First argument (index 0): Left-hand side enum OID
  - Second argument (index 1): Right-hand side enum OID

## Dependencies
- Functions called/Symbols referenced:
  - enum_cmp_internal (core comparison logic)
  - PG_GETARG_OID (argument extraction macro)
  - PG_RETURN_BOOL (result return macro)
- Called from:
  - SQL queries using < operator with enum types
  - System catalog functions
  - Query optimizer and executor

## Notes and Other Information
- Part of PostgreSQL's operator implementation framework for enum types
- Registered in the system catalogs as the implementation for the < operator on enum types
- Performance relies on enum_cmp_internal's optimization strategies
- Returns true only if left operand has lower sort order than right operand in the enum definition