# print_function_trftypes

## Location
src/backend/utils/adt/ruleutils.c: 3412 - 3439

## Overview
A static helper function that appends PostgreSQL transform types information to a string buffer for function definitions.

## Definition
```c
static void print_function_trftypes(StringInfo buf, HeapTuple proctup)
```

## Detailed Description
This function retrieves and formats the transform types associated with a PostgreSQL function and appends them to the provided string buffer. Transform types are used in PostgreSQL to specify custom type conversions for function parameters and return values. The function formats the output as "TRANSFORM FOR TYPE typename1, FOR TYPE typename2, ..." followed by a newline. If no transform types are associated with the function, nothing is appended to the buffer.

## Parameters / Member Variables
- `buf`: StringInfo buffer where the formatted transform types clause will be appended
- `proctup`: HeapTuple containing the function's metadata from the pg_proc system catalog

## Dependencies
- Functions called/Symbols referenced:
  - [get_func_trftypes](../g/get_func_trftypes.md)
  - appendStringInfoString
  - appendStringInfo
  - appendStringInfoChar
  - [format_type_be](../f/format_type_be.md)
- Called from (representative examples):
  - [pg_get_functiondef](pg_get_functiondef.md)

## Notes and Other Information
- This function is part of PostgreSQL's rule utilities system used for reconstructing function definitions
- Transform types allow custom conversion logic between SQL and procedural language types
- The function only outputs the TRANSFORM clause if there are actually transform types defined for the function
- The output format follows PostgreSQL's standard SQL syntax for CREATE FUNCTION statements