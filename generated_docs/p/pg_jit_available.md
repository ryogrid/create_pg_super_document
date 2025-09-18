# pg_jit_available

## Location
[src/backend/jit/jit.c:56-66](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/jit/jit.c#L56-L66)

## Overview
A SQL-level function that determines whether JIT (Just-In-Time) compilation is available in the current PostgreSQL backend and attempts to load the JIT provider if necessary.

## Definition


## Detailed Description
This function serves as a PostgreSQL SQL function that provides a way to check JIT availability from within SQL queries. It acts as a wrapper around the  function, which handles the actual JIT provider initialization and availability checking. When called, it will attempt to load the JIT provider if it hasn't been loaded already, then returns a boolean indicating whether JIT compilation is available for use.

## Parameters / Member Variables
This function follows the PostgreSQL function calling convention:
- Uses  macro which provides access to function arguments and context
- Returns  type which is PostgreSQL's generic data type wrapper

## Dependencies
- Functions called/Symbols referenced:
  - [provider_init](provider_init.md)
  - PG_RETURN_BOOL (macro)
- Called from (representative examples):
  - No direct references found (exposed as SQL function)

## Notes and Other Information
- This function is exposed to SQL as a system function, allowing users to programmatically check JIT availability
- Located in src/backend/jit/jit.c:56-66
- The function delegates the actual work to , which handles JIT provider loading and initialization
- Returns a boolean value that can be used in SQL queries to conditionally enable JIT-dependent functionality