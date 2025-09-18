# pg_get_function_sqlbody

## Location
src/backend/utils/adt/ruleutils.c: 3564 - 3598

## Overview
A PostgreSQL SQL function that returns the formatted SQL body of a function as text, given the function's OID.

## Definition
```c
Datum pg_get_function_sqlbody(PG_FUNCTION_ARGS)
```

## Detailed Description
This function provides a SQL-accessible interface to retrieve the formatted SQL body of a PostgreSQL function. It takes a function OID as input and returns the function's SQL body as formatted text. The function first validates that the specified function exists and has a SQL body (prosqlbody attribute), then delegates the actual formatting work to the print_function_sqlbody helper function. This function is particularly useful for introspection and documentation purposes, allowing users to examine the SQL implementation of functions.

## Parameters / Member Variables
- `funcid` (PG_GETARG_OID(0)): The OID of the function whose SQL body should be retrieved

## Dependencies
- Functions called/Symbols referenced:
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md)
  - [print_function_sqlbody](print_function_sqlbody.md)
  - cstring_to_text_with_len
  - PG_RETURN_TEXT_P
- Called from (representative examples):
  - No direct references found (likely called via SQL interface)

## Notes and Other Information
- Returns NULL if the function doesn't exist or doesn't have a SQL body
- The function only works with SQL functions that have their body stored in the prosqlbody attribute
- This is typically exposed to SQL as pg_get_function_sqlbody(funcid)
- The returned text includes proper formatting with indentation for atomic blocks and appropriate separators
- Used primarily for system introspection, debugging, and documentation generation
- Complements other pg_get_function_* functions in the PostgreSQL system catalog interface