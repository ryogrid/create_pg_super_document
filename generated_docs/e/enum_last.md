# enum_last

## Location
src/backend/utils/adt/enum.c: 466 - 495

## Overview
A PostgreSQL built-in function that returns the last (maximum) value of an enum type.

## Definition
```c
Datum enum_last(PG_FUNCTION_ARGS)
```

## Detailed Description
The `enum_last` function implements the SQL function that returns the last value in the sort order of an enum type. Like `enum_first`, it determines the enum type from the function call expression tree rather than examining the actual argument value, allowing the argument to be NULL. The function uses `enum_endpoint` with `BackwardScanDirection` to efficiently find the maximum enum value using the pg_enum system catalog's sort order index.

The function includes the same error handling as `enum_first` for cases where the enum type cannot be determined or when the enum contains no values.

## Parameters / Member Variables
- `fcinfo`: Function call information structure containing metadata about the function call

## Dependencies
- Functions called/Symbols referenced:
  - get_fn_expr_argtype
  - enum_endpoint
  - BackwardScanDirection
  - ereport
  - format_type_be
  - PG_RETURN_OID
- Called from:
  - SQL queries using enum_last() function

## Notes and Other Information
- This is a PostgreSQL built-in function accessible from SQL
- The actual argument value is not examined; the function derives the enum type from the expression tree
- Raises an error if the enum type cannot be determined from the calling context
- Raises an error if the enum type contains no values
- Returns the OID of the last enum value, which can be used in SQL operations
- Symmetric counterpart to `enum_first`, using backward scan direction instead of forward