# regexp_matches

## Location
[src/backend/utils/adt/regexp.c:1367-1415](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regexp.c#L1367-L1415)

## Overview
Returns a table of all matches of a regular expression pattern within a string, implementing PostgreSQL's regexp_matches() SQL function as a set-returning function (SRF).

## Definition
```c
Datum regexp_matches(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the SQL regexp_matches() function, which finds all matches of a regular expression pattern within a text string and returns them as a table. It uses PostgreSQL's Set-Returning Function (SRF) framework to return multiple rows. The function handles:
- Pattern compilation and matching setup via setup_regexp_matches
- Iterative return of match results using build_regexp_match_result
- Memory management in the multi-call context
- Optional flags parameter for regex options

The function operates in two phases:
1. First call: Sets up the regex matching context, parses flags, and initializes the SRF
2. Subsequent calls: Returns each match result until all matches are exhausted

## Parameters / Member Variables
- Uses PostgreSQL's standard function call interface (PG_FUNCTION_ARGS)
- PG_GETARG_TEXT_PP(1): The regular expression pattern
- PG_GETARG_TEXT_PP_IF_EXISTS(2): Optional flags string for regex options
- PG_GETARG_TEXT_P_COPY(0): The input text to search (copied to multi-call context)

## Dependencies
- Functions called/Symbols referenced:
  - [setup_regexp_matches](../s/setup_regexp_matches.md)
  - [build_regexp_match_result](../b/build_regexp_match_result.md)
  - [parse_re_flags](../p/parse_re_flags.md)
  - SRF_IS_FIRSTCALL, SRF_FIRSTCALL_INIT, SRF_PERCALL_SETUP, SRF_RETURN_NEXT, SRF_RETURN_DONE
  - PG_GET_COLLATION
- Called from (representative examples):
  - [regexp_matches_no_flags](regexp_matches_no_flags.md)

## Notes and Other Information
- This function is located in src/backend/utils/adt/regexp.c at lines 1367-1415
- Uses PostgreSQL's SRF (Set-Returning Function) framework to return multiple rows
- Memory allocation occurs in the multi-call memory context to persist across function calls
- Pre-allocates workspace arrays (elems, nulls) for efficient result building
- Returns ArrayType results containing the captured groups for each match