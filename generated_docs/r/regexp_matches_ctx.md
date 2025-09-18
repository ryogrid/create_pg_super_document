# regexp_matches_ctx

## Location
src/backend/utils/adt/regexp.c: 52 - 67

## Overview
The regexp_matches_ctx structure maintains cross-call state for PostgreSQL's regexp_match and regexp_split functions, enabling efficient processing of multiple matches within a single string.

## Definition


## Detailed Description
This structure serves as a context container for set-returning functions that process regular expression matches. It maintains all necessary state information between function calls, including the original string, match locations, and workspace buffers. The structure is designed to handle multiple matches efficiently by pre-computing all match positions and then returning them one by one in subsequent function calls. It also handles character encoding conversions and provides workspace for constructing result arrays.

## Parameters / Member Variables
- : Pointer to the original input string in PostgreSQL's TEXT format
- : Total number of locations where the regular expression pattern matched in the string
- : Number of capturing subpatterns (parenthesized groups) in the regular expression
- : Array storing start and end+1 character indexes for each match and subpattern (size: nmatches * npatterns * 2)
- : Index of the next match to be processed and returned (0-based)
- : Workspace array for constructing result tuples, with npatterns elements
- : Boolean array indicating null values in result tuples, with npatterns elements  
- : Wide-character (pg_wchar) version of the original string for proper Unicode handling
- : Buffer used for character encoding conversions when needed
- : Size of the conversion buffer in bytes

## Dependencies
- Functions called/Symbols referenced:
  - text (PostgreSQL TEXT type)
  - Datum (PostgreSQL datum type)
  - pg_wchar (PostgreSQL wide character type)
- Called from (representative examples):
  - regexp_count
  - regexp_instr  
  - regexp_match
  - regexp_matches
  - regexp_matches_no_flags
  - setup_regexp_matches
  - build_regexp_match_result
  - regexp_split_to_table
  - regexp_split_to_array
  - build_regexp_split_result
  - regexp_substr

## Notes and Other Information
This structure is critical for the efficient implementation of PostgreSQL's set-returning regular expression functions. By pre-computing all matches and storing them in the context, the system avoids repeated regex execution for each returned row. The structure handles both simple matching and complex scenarios with multiple capturing groups. Memory management for this structure is handled through PostgreSQL's memory context system, ensuring proper cleanup when the function completes.