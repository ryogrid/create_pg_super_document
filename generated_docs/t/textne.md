# textne

## Location
[src/backend/utils/adt/varlena.c:1678-1730](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L1678-L1730)

## Overview
PostgreSQL function implementing text inequality comparison (`<>` operator) with the same optimization strategies as texteq but returning the logical inverse of the equality result.

## Definition

```c
Datum
textne(PG_FUNCTION_ARGS)
```
## Detailed Description
`textne` implements the PostgreSQL `<>` or `!=` operator for text data types. It mirrors the optimization strategy of `texteq` exactly, using fast bitwise comparison for C locale and deterministic collations, and falling back to `text_cmp` for non-deterministic collations. The key difference is that it returns the logical negation of the equality test result. Like `texteq`, it includes optimizations for early detection of inequality by comparing string lengths first, and proper memory management for toasted values.

## Parameters / Member Variables
- Function uses PG_FUNCTION_ARGS macro for argument access:
  - arg1: First text value (accessed via PG_GETARG_TEXT_PP(0))
  - arg2: Second text value (accessed via PG_GETARG_TEXT_PP(1))  
  - Collation obtained via PG_GET_COLLATION()

## Dependencies
- Functions called/Symbols referenced:
  - PG_GET_COLLATION
  - [check_collation_set](../c/check_collation_set.md)
  - [lc_collate_is_c](../l/lc_collate_is_c.md)
  - [pg_newlocale_from_collation](../p/pg_newlocale_from_collation.md)
  - [pg_locale_deterministic](../p/pg_locale_deterministic.md)
  - [toast_raw_datum_size](toast_raw_datum_size.md)
  - DatumGetTextPP
  - [text_cmp](text_cmp.md)
  - VARDATA_ANY (macro)
  - PG_FREE_IF_COPY (macro)
  - PG_RETURN_BOOL (macro)
- Called from (representative examples):
  - Currently no direct references found

## Notes and Other Information
- Shares identical optimization logic with texteq but returns inverted boolean result
- References texteq implementation in comments for shared optimization rationale
- Provides fast inequality detection through length comparison before content analysis
- Part of the complete set of text comparison operators in PostgreSQL
- Implements proper memory management to prevent leaks in repeated operations