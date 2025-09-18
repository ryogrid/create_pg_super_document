# boolne

## Location
[src/backend/utils/adt/bool.c:232-240](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/bool.c#L232-L240)

## Overview
Implements the inequality operator for PostgreSQL boolean data type, comparing two boolean values for inequality.

## Definition
```c
Datum boolne(PG_FUNCTION_ARGS)
```

## Detailed Description
The `boolne` function implements the inequality comparison operator (!=) for PostgreSQL boolean values. It takes two boolean arguments and returns true if they are different (one true and one false), and false if they are equal (both true or both false). This function is part of PostgreSQL's operator system and is used when performing inequality comparisons between boolean expressions in SQL queries.

The function provides the fundamental inequality comparison logic for boolean data types, enabling SQL operations like `WHERE column1 != column2` or `WHERE column1 <> column2` when both columns are boolean.

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
- Returns true if the arguments have different boolean values, false if they are the same
- Part of PostgreSQL's public operator routines for boolean data type
- Corresponds to the SQL inequality operators (!= and <>) for boolean values
- Complement of the `booleq` function (returns opposite result)
- Essential for boolean inequality comparisons in WHERE clauses, JOIN conditions, and other SQL constructs
- Located in `src/backend/utils/adt/bool.c` at lines 232-240