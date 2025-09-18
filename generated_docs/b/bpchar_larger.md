# bpchar_larger

## Location
[src/backend/utils/adt/varchar.c:955-972](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varchar.c#L955-L972)

## Overview
Returns the larger of two BPCHAR (blank-padded CHAR) values based on string comparison using collation-aware sorting rules.

## Definition


## Detailed Description
This function implements the PostgreSQL built-in function for finding the maximum value between two BPCHAR (blank-padded character) strings. It performs a collation-aware string comparison to determine which of the two input strings is lexicographically larger according to the current locale settings. The function handles variable-length BPCHAR values by determining their true length (excluding trailing spaces) and then comparing the actual character data using PostgreSQL's standard string comparison utilities.

The comparison respects the current collation setting, making it suitable for use in various locales and character sets. If the first argument is greater than or equal to the second, it returns the first; otherwise, it returns the second.

## Parameters / Member Variables
- : First BPCHAR value to compare (extracted using PG_GETARG_BPCHAR_PP(0))
- : Second BPCHAR value to compare (extracted using PG_GETARG_BPCHAR_PP(1))
- : True length of first BPCHAR value (excluding trailing spaces)
- : True length of second BPCHAR value (excluding trailing spaces)
- : Result of string comparison (-1, 0, or 1)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_BPCHAR_PP (argument extraction macro)
  - [bcTruelen](bcTruelen.md) (determines true length of BPCHAR)
  - [varstr_cmp](../v/varstr_cmp.md) (performs collation-aware string comparison)
  - VARDATA_ANY (extracts character data from variable-length type)
  - PG_GET_COLLATION (gets current collation setting)
  - PG_RETURN_BPCHAR_P (returns BPCHAR result)
- Called from (representative examples):
  - No direct references found (likely called through SQL function dispatch)

## Notes and Other Information
- This function is typically invoked through SQL's GREATEST() function or similar maximum operations on CHAR/BPCHAR columns
- The comparison is collation-aware, meaning results may vary based on locale settings
- Trailing spaces in BPCHAR values are ignored during comparison as per SQL standard
- Returns the first argument if both values are equal (cmp >= 0 condition)
- Part of PostgreSQL's type-specific operator implementation for BPCHAR data type