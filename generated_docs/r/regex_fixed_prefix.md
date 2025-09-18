# regex_fixed_prefix

## Location
src/backend/utils/adt/like_support.c: 1099 - 1166

## Overview
Extracts the fixed prefix portion from a regular expression pattern to support query optimization by identifying literal characters at the beginning of the regex.

## Definition


## Detailed Description
This function analyzes regular expression patterns to extract the fixed (literal) prefix portion that appears before any regex metacharacters or variable components. This analysis enables PostgreSQL's query optimizer to use index scans when regex patterns start with literal characters, significantly improving query performance for patterns like '^literal_text.*'.

The function delegates the actual prefix extraction to the `regexp_fixed_prefix` function from the regex engine, which handles the complex task of parsing regex syntax to identify the literal prefix. It also calculates selectivity estimates for the remaining portion of the pattern after the prefix.

## Parameters / Member Variables
- `patt_const`: Input Const node containing the regular expression pattern (must be TEXT type)
- `case_insensitive`: Boolean indicating whether matching should be case-insensitive
- `collation`: OID of the collation to use for case-insensitive operations
- `prefix_const`: Output parameter set to a Const node containing the extracted prefix, or NULL if no prefix exists
- `rest_selec`: Output parameter set to selectivity estimate for the remainder of the pattern after the prefix

## Dependencies
- Functions called/Symbols referenced:
  - [regexp_fixed_prefix](regexp_fixed_prefix.md): Core regex engine function that extracts literal prefix from regex pattern
  - `DatumGetTextPP`/`TextDatumGetCString`: Extract pattern string from Const node
  - [regex_selectivity](regex_selectivity.md): Estimate selectivity of regex pattern or remaining portion
  - [string_to_const](../s/string_to_const.md): Create Const node for extracted prefix
- Called from (representative examples):
  - [pattern_fixed_prefix](../p/pattern_fixed_prefix.md): Generic pattern prefix extraction function

## Notes and Other Information
- This is a static function located in `src/backend/utils/adt/like_support.c:1099-1166`
- Only supports TEXT data type; explicitly rejects BYTEA with an error message
- The function relies on the regex engine's `regexp_fixed_prefix` for the actual analysis
- Returns `Pattern_Prefix_Exact` if the regex matches exactly one string (no variable components)
- Returns `Pattern_Prefix_Partial` if a non-empty literal prefix exists before variable regex components
- Returns `Pattern_Prefix_None` if no fixed prefix can be extracted
- For exact matches, the rest selectivity is set to 1.0 (100% selectivity)
- The function is conservative in its analysis to ensure correctness of query results
- Used by the query planner to optimize regex operations like SIMILAR TO and ~ operators