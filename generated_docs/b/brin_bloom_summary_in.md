# brin_bloom_summary_in

## Location
src/backend/access/brin/brin_bloom.c: 777 - 798

## Overview
Input function for the brin_bloom_summary PostgreSQL data type that explicitly disallows text input, as this type is designed only for internal binary representation of BRIN Bloom index summaries.

## Definition
```c
Datum brin_bloom_summary_in(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the input routine for the brin_bloom_summary data type, which is used internally by PostgreSQL to represent bloom filter summaries in BRIN (Block Range Index) bloom indexes. Rather than performing any actual input parsing, this function immediately raises an error to prevent users from directly creating values of this type through text input. This design choice reflects that brin_bloom_summary is purely an internal storage type that should only be created and manipulated through the BRIN bloom index access method functions.

## Parameters / Member Variables
- No direct parameters (uses PG_FUNCTION_ARGS macro for PostgreSQL function interface)
- Expected to receive text input (which it rejects)

## Dependencies
- Functions called/Symbols referenced:
  - ereport (for error reporting)
  - PG_RETURN_VOID
- Error codes used:
  - ERRCODE_FEATURE_NOT_SUPPORTED
- Called from (representative examples):
  - PostgreSQL type system (when attempting text input conversion)

## Notes and Other Information
- The function always throws an error with message "cannot accept a value of type pg_brin_bloom_summary"
- This is part of PostgreSQL's type system infrastructure for custom data types
- The brin_bloom_summary type stores data in binary form only
- This restriction prevents users from manually creating or manipulating bloom summaries, maintaining data integrity
- The PG_RETURN_VOID at the end is never reached but included to satisfy compiler requirements