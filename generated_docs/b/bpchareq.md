# bpchareq

## Location
src/backend/utils/adt/varchar.c: 743 - 787

## Overview
Implements equality comparison between two BPCHAR (blank-padded character) values, with proper collation support and optimization for C locale.

## Definition


## Detailed Description
This function compares two BPCHAR values for equality. It implements an optimized comparison strategy that considers the collation settings. For C locale or deterministic collations, it performs a fast bitwise comparison using memcmp() after verifying the lengths are equal. For other collations, it uses the more comprehensive varstr_cmp() function to handle locale-specific comparison rules. The function properly handles detoasting of potentially compressed values and ensures memory cleanup to prevent leaks, which is crucial for btree index operations.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro providing access to two BPCHAR arguments

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_BPCHAR_PP (macro)
  - PG_GET_COLLATION (macro)
  - check_collation_set
  - bcTruelen
  - lc_collate_is_c
  - pg_newlocale_from_collation
  - pg_locale_deterministic
  - varstr_cmp
  - VARDATA_ANY (macro)
  - memcmp
  - PG_FREE_IF_COPY (macro)
  - PG_RETURN_BOOL (macro)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Part of the BPCHAR comparison function suite used by btree indexes and query operations
- Optimizes for C locale by using fast bitwise comparison when possible
- Uses bcTruelen() to get the true length of BPCHAR values, excluding trailing spaces
- Properly handles memory management for toasted values to prevent memory leaks
- Returns a boolean Datum indicating whether the two BPCHAR values are equal
- Critical for SQL equality operations involving CHAR/BPCHAR data types