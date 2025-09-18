# brin_bloom_summary_recv

## Location
[src/backend/access/brin/brin_bloom.c:823-839](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_bloom.c#L823-L839)

## Overview
Binary input function for the brin_bloom_summary PostgreSQL data type that explicitly disallows binary input, maintaining the restriction that this type should only be used internally by the BRIN Bloom index access method.

## Definition
```c
Datum brin_bloom_summary_recv(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the binary input routine for the brin_bloom_summary data type, which is used internally by PostgreSQL to represent bloom filter summaries in BRIN (Block Range Index) bloom indexes. Like its text input counterpart (brin_bloom_summary_in), this function immediately raises an error to prevent users from directly creating values of this type through binary input. This design choice ensures that brin_bloom_summary values can only be created and manipulated through the proper BRIN bloom index access method functions, maintaining data integrity and preventing misuse.

## Parameters / Member Variables
- No direct parameters (uses PG_FUNCTION_ARGS macro for PostgreSQL function interface)
- Expected to receive binary input data (which it rejects)

## Dependencies
- Functions called/Symbols referenced:
  - ereport (for error reporting)
  - PG_RETURN_VOID
- Error codes used:
  - ERRCODE_FEATURE_NOT_SUPPORTED
- Called from (representative examples):
  - PostgreSQL type system (during binary data conversion operations)

## Notes and Other Information
- The function always throws an error with message "cannot accept a value of type pg_brin_bloom_summary"
- This is part of PostgreSQL's type system infrastructure for handling binary input/output operations
- Works in conjunction with brin_bloom_summary_in to completely prevent external creation of bloom summary values
- The restriction applies to both text and binary input methods, ensuring complete control over the type's lifecycle
- The PG_RETURN_VOID at the end is never reached but included to satisfy compiler requirements
- This function would typically be called during operations like COPY FROM BINARY or network protocol binary transfers