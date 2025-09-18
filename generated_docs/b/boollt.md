# boollt

## Location
[src/backend/utils/adt/bool.c:241-249](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/bool.c#L241-L249)

## Overview
Implements the less-than operator for PostgreSQL boolean data type, providing ordering comparison where false is considered less than true.

## Definition
```c
Datum boollt(PG_FUNCTION_ARGS)
```

## Detailed Description
The `boollt` function implements the less-than comparison operator (<) for PostgreSQL boolean values. It defines an ordering for boolean values where false is considered to be less than true. This function returns true if the first argument is false and the second argument is true, and false in all other cases (true < false, true < true, false < false).

This function is part of PostgreSQL's operator system and enables ordering operations on boolean columns, such as sorting and range comparisons. The boolean ordering follows the conventional interpretation where false (0) < true (1).

## Parameters / Member Variables
- First input parameter (accessed via `PG_GETARG_BOOL(0)`): The first boolean value to compare
- Second input parameter (accessed via `PG_GETARG_BOOL(1)`): The second boolean value to compare

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_BOOL` - Retrieves boolean arguments from function call (used twice)
  - `PG_RETURN_BOOL` - Returns the boolean result of the comparison

- Called from (representative examples):
  - No direct references found in the analyzed codebase

## Notes and Other Information
- Returns true only when first argument is false and second argument is true
- Establishes the ordering: false < true
- Part of PostgreSQL's public operator routines for boolean data type
- Corresponds to the SQL less-than operator (<) for boolean values
- Enables ORDER BY operations on boolean columns (false values appear before true values)
- Used in range queries and sorting operations involving boolean data
- Located in `src/backend/utils/adt/bool.c` at lines 241-249