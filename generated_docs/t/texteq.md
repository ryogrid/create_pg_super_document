# texteq

## Location
src/backend/utils/adt/varlena.c: 1619 - 1677

## Overview
PostgreSQL function implementing text equality comparison with optimizations for C locale and deterministic collations, supporting both bitwise and collation-aware equality testing.

## Definition


## Detailed Description
`texteq` implements the PostgreSQL `=` operator for text data types. It employs sophisticated optimization strategies to avoid expensive collation operations when possible. For C locale or deterministic collations, it uses fast bitwise comparison after checking string lengths for early inequality detection. For non-deterministic collations, it falls back to `text_cmp` for proper collation-aware comparison. The function includes memory management for toasted (compressed/out-of-line) values and implements the PostgreSQL function call convention.

## Parameters / Member Variables
- Function uses PG_FUNCTION_ARGS macro for argument access:
  - arg1: First text value (accessed via PG_GETARG_TEXT_PP(0))
  - arg2: Second text value (accessed via PG_GETARG_TEXT_PP(1))
  - Collation obtained via PG_GET_COLLATION()

## Dependencies
- Functions called/Symbols referenced:
  - PG_GET_COLLATION
  - check_collation_set
  - lc_collate_is_c
  - pg_newlocale_from_collation
  - pg_locale_deterministic
  - toast_raw_datum_size
  - DatumGetTextPP
  - text_cmp
  - VARDATA_ANY (macro)
  - PG_FREE_IF_COPY (macro)
  - PG_RETURN_BOOL (macro)
- Called from (representative examples):
  - text_isequal
  - texteqfast

## Notes and Other Information
- Optimizes equality testing by checking lengths before content comparison
- Handles both regular and toasted (compressed) text values efficiently
- Provides significant performance improvement for C locale comparisons
- Part of btree indexing infrastructure, marked as leakproof for security
- Implements proper memory management to prevent leaks in index operations