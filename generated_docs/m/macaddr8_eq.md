# macaddr8_eq

## Location
src/backend/utils/adt/mac8.c: 356 - 364

## Overview
PostgreSQL function that implements the equality (=) comparison operator for macaddr8 values, returning a boolean result.

## Definition
```c
Datum macaddr8_eq(PG_FUNCTION_ARGS)
```

## Detailed Description
This function provides the PostgreSQL function manager interface for the equality comparison operation between two macaddr8 values. It extracts two macaddr8 arguments from the PostgreSQL function call context and uses `macaddr8_cmp_internal` to perform the comparison, returning true if both arguments are lexicographically equal (all bytes match).

The function is part of PostgreSQL's type system and is typically invoked through the = operator in SQL queries. It follows the standard PostgreSQL fmgr calling convention and returns a boolean Datum. This function is particularly important for hash joins, hash indexes, and exact match queries.

## Parameters / Member Variables
- Uses `PG_FUNCTION_ARGS` macro which provides access to:
  - First argument: macaddr8 value (accessed via `PG_GETARG_MACADDR8_P(0)`)
  - Second argument: macaddr8 value (accessed via `PG_GETARG_MACADDR8_P(1)`)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_MACADDR8_P (macro for extracting macaddr8 arguments)
  - [macaddr8_cmp_internal](macaddr8_cmp_internal.md) (performs actual comparison)
  - PG_RETURN_BOOL (macro for returning boolean result)
  - macaddr8 (structure type)
- Called from (representative examples):
  - SQL = operator expressions
  - Hash join operations
  - Hash index lookups
  - WHERE clause equality comparisons

## Notes and Other Information
- Returns a Datum containing a boolean value (true if a1 == a2, false otherwise)
- This is a PostgreSQL fmgr v1 calling convention function
- Used internally by PostgreSQL's query engine when processing = operator for macaddr8 types
- The function is likely registered in pg_proc system catalog and associated with the = operator in pg_operator
- Essential for hash-based operations and exact match queries
- Part of the complete set of comparison operators for macaddr8 data type