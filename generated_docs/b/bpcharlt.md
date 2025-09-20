# bpcharlt

## Location
[src/backend/utils/adt/varchar.c:833-853](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varchar.c#L833-L853)

## Overview
Implements less-than comparison between two BPCHAR (blank-padded character) values using proper collation-aware string comparison.

## Definition

```c
Datum
bpcharlt(PG_FUNCTION_ARGS)
```
## Detailed Description
This function compares two BPCHAR values to determine if the first is lexicographically less than the second. Unlike the equality/inequality functions (bpchareq/bpcharne), this function always uses the full varstr_cmp() function since ordering comparisons cannot be optimized with simple bitwise operations - they require proper collation-aware string comparison to handle locale-specific sorting rules. The function extracts the true length of each BPCHAR value (excluding trailing spaces) and performs a collation-sensitive comparison, returning true if the first argument is less than the second.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro providing access to two BPCHAR arguments

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_BPCHAR_PP (macro)
  - [bcTruelen](bcTruelen.md)
  - [varstr_cmp](../v/varstr_cmp.md)
  - PG_GET_COLLATION (macro)
  - VARDATA_ANY (macro)
  - PG_FREE_IF_COPY (macro)
  - PG_RETURN_BOOL (macro)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Part of the BPCHAR ordering comparison operators (< <= > >=) used in SQL
- Always uses varstr_cmp() for proper collation-aware comparison, unlike equality functions that can optimize for C locale
- Uses bcTruelen() to get meaningful character length excluding trailing spaces
- Essential for ORDER BY clauses, range queries, and btree index operations involving BPCHAR
- Returns true when the first BPCHAR value is lexicographically less than the second
- Proper memory management with PG_FREE_IF_COPY to prevent memory leaks in index operations
- Does not include the collation validation that equality functions have, relying on varstr_cmp() to handle collation issues