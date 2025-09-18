# regexp_fixed_prefix

## Location
src/backend/utils/adt/regexp.c: 1979 - 2035

## Overview
Extracts a fixed prefix from a regular expression pattern, returning the longest literal string that must appear at the beginning of any string matching the regular expression.

## Definition


## Detailed Description
This function analyzes a regular expression to determine if it has a fixed prefix - a literal string that must appear at the beginning of any matching text. This optimization is crucial for PostgreSQL's query planner as it allows index scans to be used more efficiently when searching for patterns that start with literal text.

The function compiles the regular expression with appropriate flags, then uses the underlying regex library's prefix extraction capabilities () to identify any fixed prefix. If the entire pattern is a literal string (no regex metacharacters), it sets the  flag to indicate a complete match rather than just a prefix.

The extracted prefix is converted from the internal wide character representation back to the database encoding before being returned as a palloc'd string that the caller must free.

## Parameters / Member Variables
- : The input regular expression pattern as a PostgreSQL text datum
- : Boolean flag indicating whether the regex should be case-insensitive
- : The collation OID to use for character comparisons and case folding
- : Output parameter set to true if the entire pattern is an exact literal match (not just a prefix)

## Dependencies
- Functions called/Symbols referenced:
  - RE_compile_and_cache
  - pg_regprefix
  - pg_regerror
  - pg_database_encoding_max_length
  - pg_wchar2mb_with_len
  - regex_t (type)
  - REG_ADVANCED, REG_ICASE, REG_NOSUB (constants)
  - REG_NOMATCH, REG_PREFIX, REG_EXACT (result codes)
- Called from (representative examples):
  - regex_fixed_prefix (in src/backend/utils/adt/like_support.c)

## Notes and Other Information
- Returns NULL if no fixed prefix can be extracted from the pattern
- The returned string is palloc'd and must be freed by the caller
- Used primarily by the query optimizer to enable index scans on regex patterns with literal prefixes
- Handles both case-sensitive and case-insensitive matching through compilation flags
- Supports PostgreSQL's advanced regex features through the REG_ADVANCED flag
- Error handling includes proper reporting of regex compilation failures with descriptive messages