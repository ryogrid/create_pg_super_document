# cash_ne

## Location
[src/backend/utils/adt/cash.c:625-633](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/cash.c#L625-L633)

## Overview
Implements the inequality comparison operator (!=, <>) for PostgreSQL's cash/money data type.

## Definition

```c
Datum
cash_ne(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function performs inequality comparison between two cash values in PostgreSQL. It takes two cash arguments and returns a boolean result indicating whether the two values are not equal. This function is part of the comparison operator family for the money data type and is used when SQL queries contain inequality comparisons (using != or <> operators) between cash/money values.

The function performs a simple numerical inequality comparison since cash values are internally represented as 64-bit integers, making the comparison both efficient and straightforward.

## Parameters / Member Variables
- Input 1: First cash value retrieved via 
- Input 2: Second cash value retrieved via 
- Output: Boolean result indicating inequality

## Dependencies
- Functions called/Symbols referenced:
  -  (data type)
  -  (macro to extract cash arguments)
  -  (macro to return boolean result)
- Called from:
  - Used internally by PostgreSQL's operator system for '!=' and '<>' comparisons on money type

## Notes and Other Information
- Part of the comparison functions family for cash data type
- Performs direct integer inequality comparison since cash is stored as int64
- Used by SQL inequality operators (!= and <>) for money/cash data types
- Located in src/backend/utils/adt/cash.c:624-631
- Simple and efficient implementation due to internal integer representation

## Simplified Source

```c
Datum
cash_ne(PG_FUNCTION_ARGS)
{
    Cash c1 = PG_GETARG_CASH(0);
    Cash c2 = PG_GETARG_CASH(1);

    PG_RETURN_BOOL(c1 != c2);
}
```