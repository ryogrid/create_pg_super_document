# bytea_bit_count

## Location
[src/backend/utils/adt/varlena.c:3151-3164](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L3151-L3164)

## Overview
A PostgreSQL function that counts the number of set bits (1s) in a bytea value, implementing the SQL BIT_COUNT() function for binary string data types.

## Definition

```c
Datum
bytea_bit_count(PG_FUNCTION_ARGS)
```
## Detailed Description
The `bytea_bit_count` function implements the SQL standard BIT_COUNT() function for bytea data types. It counts the total number of bits that are set to 1 across all bytes in the input bytea value. The function uses PostgreSQL's optimized `pg_popcount` function to efficiently count the set bits in the binary data. This is useful for bit manipulation operations, bitmap analysis, and various algorithmic applications that work with binary data.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - Argument 0: `bytea *t1` - The input bytea whose bits are to be counted

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_BYTEA_PP (for extracting the bytea argument)
  - VARDATA_ANY (for getting pointer to the actual data within the bytea)
  - VARSIZE_ANY_EXHDR (for getting the size of the data excluding headers)
  - [pg_popcount](../p/pg_popcount.md) (for efficiently counting set bits)
  - PG_RETURN_INT64 (for returning 64-bit integer result)
- Called from:
  - SQL BIT_COUNT() function invocations on bytea data

## Notes and Other Information
- Returns a 64-bit integer to handle large binary data sizes
- Uses the highly optimized `pg_popcount` function which may utilize hardware instructions on supported platforms
- Operates on the raw binary data excluding PostgreSQL's internal headers
- Part of the SQL standard bit manipulation functions
- Efficient implementation suitable for large bytea values
- Located in src/backend/utils/adt/varlena.c:3151-3164