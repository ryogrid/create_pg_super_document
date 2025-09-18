# regexp_split_to_array_no_flags

## Location
src/backend/utils/adt/regexp.c: 1805 - 1816

## Overview
A wrapper function for regexp_split_to_array that provides a two-argument interface without regex flags parameter.

## Definition


## Detailed Description
This function serves as a simple wrapper around regexp_split_to_array, providing a two-parameter interface that omits the optional flags parameter. It directly passes all function call information (fcinfo) to regexp_split_to_array without any processing or modification. Like its table counterpart, this separation exists primarily to satisfy PostgreSQL's opr_sanity regression test requirements, which expect distinct function signatures for different argument counts.

## Parameters / Member Variables
- : Standard PostgreSQL function arguments containing:
  - Argument 0: Input text string to split
  - Argument 1: Regular expression pattern (text)

## Dependencies
- Functions called/Symbols referenced:
  - [regexp_split_to_array](regexp_split_to_array.md)
- Called from:
  - SQL functions with 2-argument signature

## Notes and Other Information
- Exists solely as a wrapper to satisfy PostgreSQL's opr_sanity regression test
- Provides a clean two-argument interface by omitting the optional flags parameter
- Located at src/backend/utils/adt/regexp.c:1805-1816
- Comment indicates separation is specifically for regression test compliance