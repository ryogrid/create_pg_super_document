# btint28cmp

## Location
[src/backend/access/nbtree/nbtcompare.c:231-244](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtcompare.c#L231-L244)

## Overview
A B-tree comparison function that compares a 2-byte (int16) integer with an 8-byte (int64) integer, returning the ordering relationship between the two values.

## Definition
```c
Datum btint28cmp(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the comparison logic for PostgreSQLs B-tree index operations when comparing int2 (smallint) values with int8 (bigint) values. It extracts the two arguments using PostgreSQLs function call interface, performs a three-way comparison, and returns a standardized integer result indicating the ordering relationship.

The function follows PostgreSQLs standard comparison function convention:
- Returns a positive value (A_GREATER_THAN_B) if the first argument is greater than the second
- Returns 0 if both arguments are equal
- Returns a negative value (A_LESS_THAN_B) if the first argument is less than the second

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: PostgreSQLs standard function argument interface containing:
  - First argument (index 0): int16 value (2-byte integer)
  - Second argument (index 1): int64 value (8-byte integer)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT16: Extracts int16 argument from function call context
  - PG_GETARG_INT64: Extracts int64 argument from function call context
  - PG_RETURN_INT32: Returns int32 result to PostgreSQL
  - A_GREATER_THAN_B: Constant for greater than comparison result
  - A_LESS_THAN_B: Constant for less than comparison result
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This function is part of PostgreSQLs B-tree comparison function family for mixed integer type comparisons
- The function handles cross-type comparison between int2 and int8 data types
- Located in src/backend/access/nbtree/nbtcompare.c:230-242
- Uses PostgreSQLs standard three-way comparison result convention for B-tree operations

## Simplified Source

```c
Datum btint28cmp(PG_FUNCTION_ARGS) {
    // Extract int16 and int64 arguments
    int16 a = PG_GETARG_INT16(0);
    int64 b = PG_GETARG_INT64(1);

    // Three-way comparison: return positive, zero, or negative
    if (a > b)
        return A_GREATER_THAN_B;
    else if (a == b)
        return 0;
    else
        return A_LESS_THAN_B;
}
```