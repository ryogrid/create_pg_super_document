# texteq

## Location
[src/backend/utils/adt/varlena.c:1619-1677](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L1619-L1677)

## Overview
PostgreSQL function implementing text equality comparison with optimizations for C locale and deterministic collations, supporting both bitwise and collation-aware equality testing.

## Definition

```c
Datum
texteq(PG_FUNCTION_ARGS)
```
## Detailed Description
`texteq` implements the PostgreSQL `=` operator for text data types. It employs sophisticated optimization strategies to avoid expensive collation operations when possible. For C locale or deterministic collations, it uses fast bitwise comparison after checking string lengths for early inequality detection. For non-deterministic collations, it falls back to `text_cmp` for proper collation-aware comparison. The function includes memory management for toasted (compressed/out-of-line) values and implements the PostgreSQL function call convention.

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
  - [text_isequal](text_isequal.md)
  - [texteqfast](texteqfast.md)

## Notes and Other Information
- Optimizes equality testing by checking lengths before content comparison
- Handles both regular and toasted (compressed) text values efficiently
- Provides significant performance improvement for C locale comparisons
- Part of btree indexing infrastructure, marked as leakproof for security
- Implements proper memory management to prevent leaks in index operations

## Simplified Source

```c
Datum texteq(PG_FUNCTION_ARGS) {
    Oid collid = PG_GET_COLLATION();
    bool result;

    // Check collation properties for optimization
    check_collation_set(collid);

    // Fast path: C locale or deterministic collation
    if (lc_collate_is_c(collid) || pg_locale_deterministic(pg_newlocale_from_collation(collid))) {
        Datum arg1 = PG_GETARG_DATUM(0);
        Datum arg2 = PG_GETARG_DATUM(1);

        // Quick length check - if lengths differ, strings are not equal
        Size len1 = toast_raw_datum_size(arg1);
        Size len2 = toast_raw_datum_size(arg2);

        if (len1 != len2) {
            result = false;
        } else {
            // Same length - compare actual content bytewise
            text *targ1 = DatumGetTextPP(arg1);
            text *targ2 = DatumGetTextPP(arg2);

            result = (memcmp(VARDATA_ANY(targ1), VARDATA_ANY(targ2), len1 - VARHDRSZ) == 0);

            // Clean up memory for toasted values
            PG_FREE_IF_COPY(targ1, 0);
            PG_FREE_IF_COPY(targ2, 1);
        }
    } else {
        // Slow path: locale-aware comparison
        text *arg1 = PG_GETARG_TEXT_PP(0);
        text *arg2 = PG_GETARG_TEXT_PP(1);

        result = (text_cmp(arg1, arg2, collid) == 0);

        PG_FREE_IF_COPY(arg1, 0);
        PG_FREE_IF_COPY(arg2, 1);
    }

    PG_RETURN_BOOL(result);
}
```