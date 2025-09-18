# like_fixed_prefix

## Location
src/backend/utils/adt/like_support.c: 992 - 1098

## Overview
Extracts the fixed prefix portion from a LIKE pattern string to support query optimization by identifying non-wildcard characters at the beginning of the pattern.

## Definition


## Detailed Description
This function analyzes LIKE patterns to extract the fixed (literal) prefix portion that appears before any wildcard characters (% or _). This analysis is crucial for PostgreSQL's query optimizer as it allows the use of index scans when patterns start with literal characters. The function handles both case-sensitive and case-insensitive matching, supports TEXT and BYTEA data types, and properly handles escaped characters.

The function processes the pattern character by character, stopping when it encounters wildcards (% or _), escape sequences, or case-varying alphabetic characters (in case-insensitive mode). It returns information about whether the pattern has no fixed prefix, a partial prefix, or represents an exact match.

## Parameters / Member Variables
- `patt_const`: Input Const node containing the LIKE pattern (TEXT or BYTEA)
- `case_insensitive`: Boolean indicating whether matching should be case-insensitive (ILIKE)
- `collation`: OID of the collation to use for case-insensitive operations
- `prefix_const`: Output parameter set to a Const node containing the extracted prefix, or NULL if no prefix exists
- `rest_selec`: Output parameter set to selectivity estimate for the remainder of the pattern after the prefix

## Dependencies
- Functions called/Symbols referenced:
  - [pg_database_encoding_max_length](../p/pg_database_encoding_max_length.md): Check if database uses multibyte encoding
  - [lc_ctype_is_c](lc_ctype_is_c.md)/`pg_newlocale_from_collation`: Locale handling for case-insensitive operations
  - `TextDatumGetCString`/`DatumGetByteaPP`: Extract pattern string from Const node
  - [pattern_char_isalpha](../p/pattern_char_isalpha.md): Check if character is alphabetic (case-varying)
  - [string_to_const](../s/string_to_const.md)/`string_to_bytea_const`: Create Const node for extracted prefix
  - [like_selectivity](like_selectivity.md): Estimate selectivity of remaining pattern portion
- Called from (representative examples):
  - [pattern_fixed_prefix](../p/pattern_fixed_prefix.md): Generic pattern prefix extraction function

## Notes and Other Information
- This is a static function located in `src/backend/utils/adt/like_support.c:992-1098`
- The function is conservative in its analysis - it may report a shorter prefix than the true fixed prefix to avoid incorrect query results
- Handles escape sequences properly (backslash followed by any character)
- For case-insensitive patterns, stops at alphabetic characters that could vary in case
- Returns `Pattern_Prefix_Exact` if the entire pattern is literal (no wildcards)
- Returns `Pattern_Prefix_Partial` if a non-empty prefix exists before wildcards
- Returns `Pattern_Prefix_None` if no fixed prefix can be extracted
- Case-insensitive matching is not supported for BYTEA data type