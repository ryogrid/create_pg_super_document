# int84eq

## Location
[src/backend/utils/adt/int8.c:170-178](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int8.c#L170-L178)

## Overview
Compares a 64-bit integer with a 32-bit integer for equality, returning true if they are equal.

## Definition

```c
Datum
int84eq(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements the equality comparison operator between an 8-byte (64-bit) integer and a 4-byte (32-bit) integer. It follows PostgreSQL's standard function calling convention using PG_FUNCTION_ARGS macro. The function extracts both integer arguments, compares them for equality, and returns a boolean result wrapped in PostgreSQL's Datum type.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - Argument 0: 64-bit integer (int64) value
  - Argument 1: 32-bit integer (int32) value

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT64: Extracts 64-bit integer from function arguments
  - PG_GETARG_INT32: Extracts 32-bit integer from function arguments
  - PG_RETURN_BOOL: Returns boolean result as PostgreSQL Datum
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Part of PostgreSQL's int8 (bigint) data type operations
- Located in src/backend/utils/adt/int8.c:170-178
- This is one of the relational operators for mixed-precision integer comparisons
- The comparison is performed directly using C's == operator after type extraction

## Simplified Source

```c
Datum int84eq(PG_FUNCTION_ARGS) {
    // Extract 64-bit and 32-bit integers from function arguments
    int64 val1 = PG_GETARG_INT64(0);
    int32 val2 = PG_GETARG_INT32(1);

    // Return boolean result of equality comparison (32-bit is promoted to 64-bit)
    PG_RETURN_BOOL(val1 == val2);
}
```