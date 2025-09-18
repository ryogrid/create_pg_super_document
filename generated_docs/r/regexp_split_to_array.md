# regexp_split_to_array

## Location
[src/backend/utils/adt/regexp.c:1766-1804](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regexp.c#L1766-L1804)

## Overview
Splits a string at matches of a regular expression pattern, returning all split-out substrings as a single array.

## Definition


## Detailed Description
This function implements array-based string splitting using regular expressions. Unlike regexp_split_to_table which returns results as multiple rows, this function collects all split substrings into a single array and returns it. The function prohibits the 'g' (global) flag in user input but internally enables global matching to find all pattern occurrences. It uses PostgreSQL's ArrayBuildState mechanism to accumulate results and build the final array. The function processes all matches in a single call, building up the array incrementally using accumArrayResult for each split substring.

## Parameters / Member Variables
- : Standard PostgreSQL function arguments containing:
  - Argument 0: Input text string to split
  - Argument 1: Regular expression pattern (text)  
  - Argument 2: Optional regex flags (text), but 'g' flag is prohibited

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TEXT_PP, PG_GETARG_TEXT_PP_IF_EXISTS
  - PG_GET_COLLATION, PG_RETURN_DATUM
  - [parse_re_flags](../p/parse_re_flags.md)
  - [setup_regexp_matches](../s/setup_regexp_matches.md)
  - [build_regexp_split_result](../b/build_regexp_split_result.md)
  - [accumArrayResult](../a/accumArrayResult.md), makeArrayResult
  - [ArrayBuildState](../A/ArrayBuildState.md), pg_re_flags, regexp_matches_ctx
- Called from:
  - [regexp_split_to_array_no_flags](regexp_split_to_array_no_flags.md)

## Notes and Other Information
- Prohibits 'g' flag in user input but internally enables global matching
- Returns all results in a single array rather than as separate rows
- Uses ArrayBuildState to efficiently build the result array
- Processes all matches in one function call, unlike the table variant
- Located at src/backend/utils/adt/regexp.c:1766-1804