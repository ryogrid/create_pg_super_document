# cash_lt

## Location
[src/backend/utils/adt/cash.c:634-642](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/cash.c#L634-L642)

## Overview
Implements the less-than comparison operator (<) for PostgreSQL's cash/money data type.

## Definition

```c
Datum
cash_lt(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function performs less-than comparison between two cash values in PostgreSQL. It takes two cash arguments and returns a boolean result indicating whether the first value is less than the second value. This function is part of the comparison operator family for the money data type and is used when SQL queries contain less-than comparisons between cash/money values.

The function performs a simple numerical less-than comparison since cash values are internally represented as 64-bit integers, making the comparison both efficient and straightforward.

## Parameters / Member Variables
- Input 1: First cash value retrieved via 
- Input 2: Second cash value retrieved via 
- Output: Boolean result indicating if first value is less than second

## Dependencies
- Functions called/Symbols referenced:
  -  (data type)
  -  (macro to extract cash arguments)
  -  (macro to return boolean result)
- Called from:
  - Used internally by PostgreSQL's operator system for '<' comparisons on money type

## Notes and Other Information
- Part of the comparison functions family for cash data type
- Performs direct integer less-than comparison since cash is stored as int64
- Used by SQL less-than operator (<) for money/cash data types
- Located in src/backend/utils/adt/cash.c:633-640
- Simple and efficient implementation due to internal integer representation
- Essential for ordering operations and range queries on money values

## Simplified Source

```c
Datum
cash_lt(PG_FUNCTION_ARGS)
{
    Cash c1 = PG_GETARG_CASH(0);
    Cash c2 = PG_GETARG_CASH(1);

    PG_RETURN_BOOL(c1 < c2);
}
```