# btint84cmp

## Location
src/backend/access/nbtree/nbtcompare.c: 189 - 202

## Overview
A B-tree comparison function that compares an 8-byte (int64) integer with a 4-byte (int32) integer, returning the ordering relationship between the two values.

## Definition
```c
Datum btint84cmp(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the comparison logic for PostgreSQLs B-tree index operations when comparing int8 (bigint) values with int4 (integer) values. It extracts the two arguments using PostgreSQLs function call interface, performs a three-way comparison, and returns a standardized integer result indicating the ordering relationship.

The function follows PostgreSQLs standard comparison function convention:
- Returns a positive value (A_GREATER_THAN_B) if the first argument is greater than the second
- Returns 0 if both arguments are equal
- Returns a negative value (A_LESS_THAN_B) if the first argument is less than the second

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: PostgreSQLs standard function argument interface containing:
  - First argument (index 0): int64 value (8-byte integer)
  - Second argument (index 1): int32 value (4-byte integer)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT64: Extracts int64 argument from function call context
  - PG_GETARG_INT32: Extracts int32 argument from function call context
  - PG_RETURN_INT32: Returns int32 result to PostgreSQL
  - A_GREATER_THAN_B: Constant for greater than comparison result
  - A_LESS_THAN_B: Constant for less than comparison result
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This function is part of PostgreSQLs B-tree comparison function family for mixed integer type comparisons
- The function handles cross-type comparison between int8 and int4 data types
- Located in src/backend/access/nbtree/nbtcompare.c:188-200
- Uses PostgreSQLs standard three-way comparison result convention for B-tree operations