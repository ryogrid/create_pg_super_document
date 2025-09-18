# bpchar_smaller

## Location
src/backend/utils/adt/varchar.c: 973 - 995

## Overview
Returns the smaller of two BPCHAR (blank-padded CHAR) values based on string comparison using collation-aware sorting rules.

## Definition
```c
Datum bpchar_smaller(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the PostgreSQL built-in function for finding the minimum value between two BPCHAR (blank-padded character) strings. It performs a collation-aware string comparison to determine which of the two input strings is lexicographically smaller according to the current locale settings. The function handles variable-length BPCHAR values by determining their true length (excluding trailing spaces) and then comparing the actual character data using PostgreSQL's standard string comparison utilities.

The comparison respects the current collation setting, making it suitable for use in various locales and character sets. If the first argument is less than or equal to the second, it returns the first; otherwise, it returns the second. This is the complement function to bpchar_larger.

## Parameters / Member Variables
- `arg1`: First BPCHAR value to compare (extracted using PG_GETARG_BPCHAR_PP(0))
- `arg2`: Second BPCHAR value to compare (extracted using PG_GETARG_BPCHAR_PP(1))
- `len1`: True length of first BPCHAR value (excluding trailing spaces)
- `len2`: True length of second BPCHAR value (excluding trailing spaces)
- `cmp`: Result of string comparison (-1, 0, or 1)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_BPCHAR_PP (argument extraction macro)
  - bcTruelen (determines true length of BPCHAR)
  - varstr_cmp (performs collation-aware string comparison)
  - VARDATA_ANY (extracts character data from variable-length type)
  - PG_GET_COLLATION (gets current collation setting)
  - PG_RETURN_BPCHAR_P (returns BPCHAR result)
- Called from (representative examples):
  - No direct references found (likely called through SQL function dispatch)

## Notes and Other Information
- This function is typically invoked through SQL's LEAST() function or similar minimum operations on CHAR/BPCHAR columns
- The comparison is collation-aware, meaning results may vary based on locale settings
- Trailing spaces in BPCHAR values are ignored during comparison as per SQL standard
- Returns the first argument if both values are equal (cmp <= 0 condition)
- Part of PostgreSQL's type-specific operator implementation for BPCHAR data type
- Functionally inverse of bpchar_larger, using <= instead of >= for the comparison result