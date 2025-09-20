# regexp_split_to_table_no_flags

## Location
[src/backend/utils/adt/regexp.c:1755-1765](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regexp.c#L1755-L1765)

## Overview
A wrapper function for regexp_split_to_table that provides a two-argument interface without regex flags parameter.

## Definition

```c
Datum
regexp_split_to_table_no_flags(PG_FUNCTION_ARGS)
```
## Detailed Description
This function serves as a simple wrapper around regexp_split_to_table, providing a two-parameter interface that omits the optional flags parameter. It directly passes all function call information (fcinfo) to regexp_split_to_table without any processing or modification. This separation exists primarily to satisfy PostgreSQL's opr_sanity regression test requirements, which expect distinct function signatures for different argument counts.

## Parameters / Member Variables
- : Standard PostgreSQL function arguments containing:
  - Argument 0: Input text string to split
  - Argument 1: Regular expression pattern (text)

## Dependencies
- Functions called/Symbols referenced:
  - [regexp_split_to_table](regexp_split_to_table.md)
- Called from:
  - SQL functions with 2-argument signature

## Notes and Other Information
- Exists solely as a wrapper to satisfy PostgreSQL's opr_sanity regression test
- Provides a clean two-argument interface by omitting the optional flags parameter
- Located at src/backend/utils/adt/regexp.c:1755-1765
- Comment indicates separation is specifically for regression test compliance