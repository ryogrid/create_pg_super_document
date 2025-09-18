# int4le

## Location
src/backend/utils/adt/int.c: 423 - 431

## Overview
A PostgreSQL built-in function that compares two 32-bit integers and returns true if the first integer is less than or equal to the second.

## Definition


## Detailed Description
The int4le function implements the "<=" (less than or equal) comparison operator for PostgreSQL's int4 (32-bit integer) data type. It takes two int4 values as arguments and returns a boolean result indicating whether the first value is less than or equal to the second value. This function is part of PostgreSQL's type system and is used internally by the SQL engine when processing "<=" comparisons between integer values.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that provides access to function arguments
  - arg1 (int32): The first integer value to compare
  - arg2 (int32): The second integer value to compare

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT32 (macro to extract int32 arguments)
  - PG_RETURN_BOOL (macro to return boolean result)
- Called from (representative examples):
  - SQL engine during query execution for "<=" comparisons
  - Expression evaluation subsystem

## Notes and Other Information
- This function is part of the core integer arithmetic operations in PostgreSQL
- It follows the standard PostgreSQL function calling convention using PG_FUNCTION_ARGS
- The function is registered in the system catalogs and can be invoked via SQL queries
- Returns true if arg1 <= arg2, false otherwise
- Located in src/backend/utils/adt/int.c:423-431