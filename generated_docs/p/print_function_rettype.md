# print_function_rettype

## Location
[src/backend/utils/adt/ruleutils.c:3214-3251](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L3214-L3251)

## Overview
A static helper function that appends a formatted representation of a function's return type to a StringInfo buffer, handling both regular functions and table functions.

## Definition
```c
static void print_function_rettype(StringInfo buf, HeapTuple proctup)
```

## Detailed Description
This internal function formats and appends a function's return type specification to the provided buffer. It handles two main scenarios: table functions (which return TABLE(...) with column specifications) and regular functions (which return a single type, optionally with SETOF). For table functions, it attempts to print the function arguments as table column definitions. If this succeeds, it formats the return type as TABLE(...). Otherwise, it falls back to the standard return type formatting with optional SETOF prefix for set-returning functions.

## Parameters / Member Variables
- `buf`: StringInfo buffer to append the formatted return type to
- `proctup`: HeapTuple containing the function's metadata from pg_proc

## Dependencies
- Functions called/Symbols referenced:
  - Form_pg_proc
  - [print_function_arguments](print_function_arguments.md)
  - [resetStringInfo](../r/resetStringInfo.md)
  - [appendBinaryStringInfo](../a/appendBinaryStringInfo.md)
- Called from (representative examples):
  - [NameHashEntry](../N/NameHashEntry.md)
  - [pg_get_functiondef](pg_get_functiondef.md)
  - [pg_get_function_result](pg_get_function_result.md)

## Notes and Other Information
- This is a static (internal) function used by other ruleutils.c functions
- Handles the complexity of table function return type formatting
- Uses a temporary StringInfo buffer (rbuf) to build the return type string before appending to the main buffer
- The function distinguishes between table functions and regular functions based on whether print_function_arguments succeeds in generating table column definitions
- For set-returning functions that aren't table functions, it prefixes the return type with 'SETOF'
- Part of the core functionality for generating CREATE FUNCTION statements and function signatures