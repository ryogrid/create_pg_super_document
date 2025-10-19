# btint48cmp

## Location
[src/backend/access/nbtree/nbtcompare.c:175-188](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtcompare.c#L175-L188)

## Overview
A PostgreSQL B-tree comparison function that compares a 32-bit integer (int4) with a 64-bit integer (int8), supporting mixed-type comparisons in index operations.

## Definition

```c
Datum
btint48cmp(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements cross-type comparison between 32-bit and 64-bit integers for PostgreSQL B-tree indexes. It takes a 32-bit integer as the first argument and a 64-bit integer as the second argument, performing the comparison with appropriate type promotion. The 32-bit value is implicitly promoted to 64-bit precision before comparison, ensuring accurate results when comparing values of different integer widths.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro providing access to two parameters:
  - First parameter: A 32-bit signed integer (int4)
  - Second parameter: A 64-bit signed integer (int8)

## Dependencies
- Functions called/Symbols referenced:
  -  (PostgreSQL 32-bit argument extraction macro)
  -  (PostgreSQL 64-bit argument extraction macro)
  -  (comparison result constant)
  -  (comparison result constant)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This function enables mixed-type comparisons between int4 and int8 data types in B-tree indexes
- Located in
- The int4 value is automatically promoted to int8 precision during comparison
- Returns standard comparison values: >0 for greater than, 0 for equal, <0 for less than
- Part of PostgreSQL's comprehensive type system support for B-tree index operations
- Follows the naming convention where '48' indicates int4-to-int8 comparison

## Simplified Source

```c
Datum
btint48cmp(PG_FUNCTION_ARGS)
{
    // Extract int32 and int64 arguments for cross-type comparison
    int32 a = PG_GETARG_INT32(0);
    int64 b = PG_GETARG_INT64(1);

    // Compare with automatic promotion of int32 to int64
    if (a > b)
        PG_RETURN_INT32(1);    // A_GREATER_THAN_B
    else if (a == b)
        PG_RETURN_INT32(0);
    else
        PG_RETURN_INT32(-1);   // A_LESS_THAN_B
}
```