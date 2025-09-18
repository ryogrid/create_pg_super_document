# bpcharne

## Location
src/backend/utils/adt/varchar.c: 788 - 832

## Overview
Implements inequality (not-equal) comparison between two BPCHAR (blank-padded character) values, with proper collation support and optimization for C locale.

## Definition


## Detailed Description
This function compares two BPCHAR values for inequality (not-equal). It follows the same optimization strategy as bpchareq but returns the opposite result. For C locale or deterministic collations, it performs a fast bitwise comparison using memcmp() and returns true if lengths differ or if the memory comparison shows differences. For other collations, it uses varstr_cmp() to handle locale-specific comparison rules and returns true if the comparison result is not zero. The function properly manages memory for toasted values and ensures cleanup to prevent memory leaks in btree operations.

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
- Complementary function to bpchareq, implementing the != operator for BPCHAR values
- Uses identical comparison logic but returns the negated result
- Optimizes for C locale with fast bitwise comparison when collation allows it
- Uses bcTruelen() to determine true character length, excluding trailing spaces
- Essential for SQL inequality operations involving CHAR/BPCHAR data types
- Part of the complete set of BPCHAR comparison operators used in queries and indexes
- Returns true when the BPCHAR values are not equal, false when they are equal