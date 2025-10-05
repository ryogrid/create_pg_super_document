# cash_pl_cash

## Location
[src/backend/utils/adt/cash.c:91-103](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/cash.c#L91-L103)

## Overview
A private inline function that performs safe addition of two Cash values with overflow detection and error reporting.

## Definition

```c
static inline Cash
cash_pl_cash(Cash c1, Cash c2)
```
## Detailed Description
The  function provides safe addition of two Cash values (64-bit signed integers representing monetary amounts). It uses PostgreSQL's overflow-safe arithmetic function  to detect integer overflow conditions. If an overflow would occur, the function reports a "money out of range" error using PostgreSQL's error reporting mechanism. This ensures that monetary calculations remain within valid bounds and prevents silent overflow errors that could corrupt financial data.

## Parameters / Member Variables
- `c1`: First Cash value to add
- `c2`: Second Cash value to add
## Dependencies
- Functions called/Symbols referenced:
  - Cash (type)
  - [pg_add_s64_overflow](../p/pg_add_s64_overflow.md) (overflow-safe addition)
  - ereport (error reporting)
  - [errcode](../e/errcode.md) (error code specification)
  - [errmsg](../e/errmsg.md) (error message formatting)
  - ERROR (error level constant)
  - ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE (specific error code)
- Called from (representative examples):
  - [cash_pl](cash_pl.md)

## Notes and Other Information
- Declared as  for performance optimization in arithmetic operations
- Uses PostgreSQL's safe arithmetic functions to prevent integer overflow
- Part of PostgreSQL's cash data type implementation for monetary arithmetic
- The error message "money out of range" provides clear indication of overflow conditions
- Returns the sum result only if no overflow occurs, otherwise throws an error

## Simplified Source

```c
static inline Cash
cash_pl_cash(Cash c1, Cash c2)
{
    Cash res;

    // Check for overflow during addition
    if (unlikely(pg_add_s64_overflow(c1, c2, &res)))
        ereport(ERROR,
                (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                 errmsg("money out of range")));

    return res;
}
```