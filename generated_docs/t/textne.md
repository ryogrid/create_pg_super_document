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

## Simplified Source
```c
Datum textne(PG_FUNCTION_ARGS)
{
    Oid collation_id = PG_GET_COLLATION();
    bool result;

    check_collation_set(collation_id);

    // Fast path for C locale and deterministic collations
    if (lc_collate_is_c(collation_id) ||
        pg_locale_deterministic(pg_newlocale_from_collation(collation_id))) {

        Datum arg1 = PG_GETARG_DATUM(0);
        Datum arg2 = PG_GETARG_DATUM(1);

        // Quick inequality check: different lengths = not equal
        Size len1 = toast_raw_datum_size(arg1);
        Size len2 = toast_raw_datum_size(arg2);

        if (len1 != len2) {
            result = true;  // Different lengths = not equal
        } else {
            // Same length: compare bytes directly
            text *text1 = DatumGetTextPP(arg1);
            text *text2 = DatumGetTextPP(arg2);

            result = (memcmp(VARDATA_ANY(text1), VARDATA_ANY(text2),
                            len1 - VARHDRSZ) != 0);

            // Cleanup memory if needed
            PG_FREE_IF_COPY(text1, 0);
            PG_FREE_IF_COPY(text2, 1);
        }
    }
    // Slow path for non-deterministic collations
    else {
        text *text1 = PG_GETARG_TEXT_PP(0);
        text *text2 = PG_GETARG_TEXT_PP(1);

        // Use collation-aware comparison
        result = (text_cmp(text1, text2, collation_id) != 0);

        PG_FREE_IF_COPY(text1, 0);
        PG_FREE_IF_COPY(text2, 1);
    }

    return PG_RETURN_BOOL(result);
}
```