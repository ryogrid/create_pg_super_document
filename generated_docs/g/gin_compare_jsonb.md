# gin_compare_jsonb

## Location
[src/backend/utils/adt/jsonb_gin.c:203-228](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb_gin.c#L203-L228)

## Overview
A GIN opclass support function that compares two text values extracted from JSONB data for ordering within the GIN index, using C collation for consistent sorting.

## Definition

```c
Datum
gin_compare_jsonb(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements the comparison operation required by the jsonb_ops GIN operator class. It compares two text arguments that represent keys, values, or other textual components extracted from JSONB data. The function uses PostgreSQL's varstr_cmp function with C collation (C_COLLATION_OID) to ensure consistent, locale-independent comparison results across different database configurations.

The function follows PostgreSQL's standard comparison semantics, returning a negative value if the first argument is less than the second, zero if they are equal, and a positive value if the first argument is greater than the second. This ordering is crucial for the internal organization of GIN index entries.

## Parameters / Member Variables
- : Standard PostgreSQL function interface accepting two text arguments:
  - arg1: First text value to compare (retrieved via PG_GETARG_TEXT_PP(0))
  - arg2: Second text value to compare (retrieved via PG_GETARG_TEXT_PP(1))

## Return Value
- Returns an int32 result indicating the comparison outcome:
  - Negative: arg1 < arg2
  - Zero: arg1 = arg2  
  - Positive: arg1 > arg2

## Dependencies
- Functions called/Symbols referenced:
  - [varstr_cmp](../v/varstr_cmp.md) (string comparison function)
  - C_COLLATION_OID (constant for C collation)
  - PG_GETARG_TEXT_PP, VARDATA_ANY, VARSIZE_ANY_EXHDR (PostgreSQL macros)
  - PG_FREE_IF_COPY, PG_RETURN_INT32 (PostgreSQL macros)
- Called from (representative examples):
  - Registered as part of the jsonb_ops GIN operator class (referenced indirectly through system catalogs)

## Notes and Other Information
- This function is part of the jsonb_ops GIN opclass infrastructure for indexing JSONB data
- Uses C collation to ensure deterministic sorting regardless of database locale settings
- The function signature follows PostgreSQL's V1 calling convention using PG_FUNCTION_ARGS
- Memory management is handled properly with PG_FREE_IF_COPY to avoid memory leaks
- The comparison is performed on the raw text data without considering JSONB-specific semantics
- This function enables the GIN index to maintain sorted order of text-based entries for efficient range queries and index maintenance