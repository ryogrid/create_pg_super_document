# repeat

## Location
[src/backend/utils/adt/oracle_compat.c:1121-1157](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/oracle_compat.c#L1121-L1157)

## Overview
Repeats a given text string a specified number of times, concatenating the results into a single output string.

## Definition
```c
Datum repeat(PG_FUNCTION_ARGS)
```

## Detailed Description
The repeat function creates a new text string by concatenating the input string with itself a specified number of times. It includes robust overflow protection to prevent memory allocation issues when dealing with large strings or high repeat counts. The function handles edge cases gracefully: negative repeat counts are treated as zero, resulting in an empty string. The implementation uses efficient memory copying and includes interrupt checking during long operations to maintain system responsiveness. Memory allocation is calculated carefully using overflow-safe arithmetic to prevent integer overflow vulnerabilities.

## Parameters / Member Variables
- `string`: The input text string to be repeated
- `count`: The number of times to repeat the string (negative values treated as 0)

## Dependencies
- Functions called/Symbols referenced:
  - [pg_mul_s32_overflow](../p/pg_mul_s32_overflow.md)
  - [pg_add_s32_overflow](../p/pg_add_s32_overflow.md)
  - AllocSizeIsValid
  - SET_VARSIZE
  - VARDATA
  - VARDATA_ANY
  - CHECK_FOR_INTERRUPTS
  - PG_RETURN_TEXT_P
- Called from (representative examples):
  - No direct references found in the analyzed codebase

## Notes and Other Information
- Located in src/backend/utils/adt/oracle_compat.c:1120-1157
- Part of PostgreSQL's Oracle compatibility layer
- Uses safe arithmetic functions to prevent integer overflow during memory size calculation
- Includes CHECK_FOR_INTERRUPTS() in the loop to handle query cancellation during long operations
- Handles negative repeat counts by setting them to zero
- Memory allocation uses AllocSizeIsValid() to ensure the calculated size is within acceptable limits
- The function efficiently copies string data using memcpy() for optimal performance
- Provides protection against memory exhaustion attacks through careful size validation