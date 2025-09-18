# float8in

## Location
src/backend/utils/adt/float.c: 357 - 387

## Overview
PostgreSQL built-in function that converts string input to double-precision floating-point values (float8) using the standard input interface.

## Definition


## Detailed Description
The float8in function is PostgreSQL's standard input function for the float8 (double precision) data type. It serves as the public interface for converting textual representations of double-precision floating-point numbers into PostgreSQL's internal float8 format. The function acts as a wrapper around the more comprehensive float8in_internal function, providing the standard PostgreSQL function calling interface while delegating the actual parsing logic to the internal implementation.

The function extracts the input string from the function arguments and passes it to float8in_internal with appropriate parameters including the type name "double precision" for error reporting and the function's error context for proper error handling integration.

## Parameters / Member Variables
- Function accepts PostgreSQL function arguments via PG_FUNCTION_ARGS macro
- : Input string containing the double-precision number to be parsed, extracted using PG_GETARG_CSTRING(0)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CSTRING (argument extraction)
  - [float8in_internal](float8in_internal.md) (core parsing logic)
  - PG_RETURN_FLOAT8 (return value macro)
- Called from (representative examples):
  - [numeric_float8](../n/numeric_float8.md) (from numeric.c:4663)

## Notes and Other Information
- Registered in PostgreSQL's system catalogs as the standard input function for float8/double precision type
- Uses "double precision" as the type name in error messages for consistency with SQL standard terminology
- Passes the error context from the function call info to enable proper error reporting and soft error handling
- Acts as a thin wrapper around float8in_internal, which contains the actual parsing implementation
- Part of PostgreSQL's type input/output system that enables automatic string-to-float8 conversions in SQL operations