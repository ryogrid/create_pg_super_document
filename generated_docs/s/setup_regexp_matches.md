# setup_regexp_matches

## Location
src/backend/utils/adt/regexp.c: 1442 - 1645

## Overview
Performs the initial pattern matching setup for regexp_match, regexp_matches, regexp_split, and related functions by compiling the regex pattern and finding all matches in the input string.

## Definition
```c
static regexp_matches_ctx *setup_regexp_matches(text *orig_str, text *pattern, pg_re_flags *re_flags,
                                               int start_search, Oid collation,
                                               bool use_subpatterns,
                                               bool ignore_degenerate,
                                               bool fetching_unmatched)
```

## Detailed Description
This function is the core regex matching engine for PostgreSQL's regular expression functions. It:

1. Converts the input string to wide character format for proper Unicode handling
2. Compiles the regex pattern with appropriate flags and caching
3. Performs all pattern matching in one operation to avoid recompilation overhead
4. Stores match locations and subpattern information in a context structure
5. Handles memory management for variable-length result arrays
6. Supports both global (all matches) and single-match modes
7. Manages character encoding conversions for multi-byte character sets

The function optimizes performance by doing all matching upfront and caching results, rather than re-executing the regex on each function call. It also handles degenerate (zero-length) matches and supports fetching unmatched portions for split operations.

## Parameters / Member Variables
- `orig_str`: The original input text string to search within
- `pattern`: The regular expression pattern to match against  
- `re_flags`: Compiled regex flags structure containing options like global matching
- `start_search`: Character offset in orig_str where matching should begin
- `collation`: Database collation to use for pattern matching
- `use_subpatterns`: Whether to collect data about parenthesized subexpression matches
- `ignore_degenerate`: Whether to ignore zero-length matches
- `fetching_unmatched`: Whether caller wants to fetch unmatched substring portions

## Dependencies
- Functions called/Symbols referenced:
  - RE_compile_and_cache
  - RE_wchar_execute  
  - pg_mb2wchar_with_len
  - pg_database_encoding_max_length
  - repalloc, palloc, pfree
- Called from (representative examples):
  - regexp_matches
  - regexp_match
  - regexp_count
  - regexp_instr
  - regexp_substr
  - regexp_split_to_table
  - regexp_split_to_array

## Notes and Other Information
- Located in src/backend/utils/adt/regexp.c at lines 1442-1645
- Returns a dynamically allocated regexp_matches_ctx structure containing all match results
- Uses exponential array growth (2^n-1 pattern) to efficiently handle varying numbers of matches
- Includes protection against excessive memory usage with MaxAllocSize checking
- Handles both single-byte and multi-byte character encodings efficiently
- For multi-byte encodings, maintains both wide character and original byte representations
- The function is static (internal to the regexp.c module)