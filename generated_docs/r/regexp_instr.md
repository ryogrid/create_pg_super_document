# regexp_instr

## Location
src/backend/utils/adt/regexp.c: 1152 - 1244

## Overview
Returns the position (1-based index) of a regular expression match within a string, with support for multiple optional parameters including match occurrence, start position, and subexpression selection.

## Definition
```c
Datum regexp_instr(PG_FUNCTION_ARGS)
```

## Detailed Description
The regexp_instr function is a comprehensive regular expression position-finding function that locates pattern matches within text strings. It supports various parameters to control matching behavior:

- Finds the nth occurrence of a pattern match
- Allows specification of starting position within the string
- Can return either start or end position of matches (controlled by endoption)
- Supports subexpression matching for capturing groups
- Validates all input parameters and provides detailed error messages for invalid values

The function uses PostgreSQL's internal regular expression engine and integrates with the collation system for locale-aware matching. It returns 0 when no match is found or when the requested occurrence/subexpression doesn't exist.

## Parameters / Member Variables
- `str`: Input text string to search within (argument 0)
- `pattern`: Regular expression pattern to match (argument 1) 
- `start`: Starting position for search (1-based, argument 2, default: 1)
- `n`: Which occurrence of the match to return (argument 3, default: 1)
- `endoption`: Whether to return start (0) or end (1) position (argument 4, default: 0)
- `flags`: Regular expression flags for matching behavior (argument 5, optional)
- `subexpr`: Which subexpression/capture group to return position for (argument 6, default: 0 for whole match)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TEXT_PP_IF_EXISTS: Extract optional text arguments
  - parse_re_flags: Parse regular expression flags
  - setup_regexp_matches: Set up pattern matching context
  - PG_GET_COLLATION: Get current collation for locale-aware matching
  - PG_NARGS: Get number of function arguments
  - Various PostgreSQL error reporting functions

- Called from (representative examples):
  - regexp_instr_no_start: Wrapper without start parameter
  - regexp_instr_no_n: Wrapper without occurrence parameter
  - regexp_instr_no_endoption: Wrapper without end option parameter
  - regexp_instr_no_flags: Wrapper without flags parameter
  - regexp_instr_no_subexpr: Wrapper without subexpression parameter

## Notes and Other Information
- Returns 1-based positions (PostgreSQL SQL standard)
- Returns 0 when no match is found or parameters are out of range
- Does not support the global 'g' flag (generates error if specified)
- Internally enables global matching to find all occurrences up to the nth match
- Validates that start > 0, n > 0, endoption ∈ {0,1}, and subexpr ≥ 0
- Part of PostgreSQL's SQL standard regular expression function suite
- Used by several wrapper functions that provide different parameter combinations