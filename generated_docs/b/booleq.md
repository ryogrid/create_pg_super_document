# booleq

## Location
[src/backend/utils/adt/bool.c:223-231](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/bool.c#L223-L231)

## Overview
Implements the equality operator for PostgreSQL boolean data type, comparing two boolean values for equality.

## Definition
```c
Datum booleq(PG_FUNCTION_ARGS)
```

## Detailed Description
The `booleq` function implements the equality comparison operator (=) for PostgreSQL boolean values. It takes two boolean arguments and returns true if they are equal (both true or both false), and false if they are different. This function is part of PostgreSQL's operator system and is used when performing equality comparisons between boolean expressions in SQL queries.

The function provides the fundamental equality comparison logic for boolean data types, enabling SQL operations like `WHERE column1 = column2` when both columns are boolean.

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
- Returns true if both arguments have the same boolean value, false otherwise
- Part of PostgreSQL's public operator routines for boolean data type
- Corresponds to the SQL equality operator (=) for boolean values
- Essential for boolean comparisons in WHERE clauses, JOIN conditions, and other SQL constructs
- Located in `src/backend/utils/adt/bool.c` at lines 223-231

## Simplified Source

```c
Datum booleq(PG_FUNCTION_ARGS) {
    bool arg1 = PG_GETARG_BOOL(0);
    bool arg2 = PG_GETARG_BOOL(1);

    // Return true if both arguments are equal, false otherwise
    PG_RETURN_BOOL(arg1 == arg2);
}
```

This function implements the equality operator (=) for boolean values. It extracts two boolean arguments and performs a simple equality comparison, returning true if both values are the same (both true or both false) and false if they differ.