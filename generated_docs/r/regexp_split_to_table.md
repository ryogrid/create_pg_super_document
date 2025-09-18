# regexp_split_to_table

## Location
src/backend/utils/adt/regexp.c: 1702 - 1754

## Overview
Splits a string at matches of a regular expression pattern, returning the split-out substrings as a table (set-returning function).

## Definition


## Detailed Description
This function implements a PostgreSQL set-returning function (SRF) that splits an input string using a regular expression pattern and returns each split substring as a separate row. The function uses the SRF framework to manage state across multiple calls, storing the regexp matching context in the function's multi-call memory context. Unlike the standard behavior where users can specify the 'g' (global) flag, this function prohibits the global flag but internally forces global matching to find all occurrences of the pattern. The function processes matches sequentially, returning one split result per call until all matches have been processed.

## Parameters / Member Variables
- : Standard PostgreSQL function arguments containing:
  - Argument 0: Input text string to split
  - Argument 1: Regular expression pattern (text)
  - Argument 2: Optional regex flags (text), but 'g' flag is prohibited

## Dependencies
- Functions called/Symbols referenced:
  - SRF_IS_FIRSTCALL, SRF_FIRSTCALL_INIT, SRF_PERCALL_SETUP
  - SRF_RETURN_NEXT, SRF_RETURN_DONE
  - PG_GETARG_TEXT_PP, PG_GETARG_TEXT_PP_IF_EXISTS, PG_GETARG_TEXT_P_COPY
  - PG_GET_COLLATION
  - [parse_re_flags](../p/parse_re_flags.md)
  - [setup_regexp_matches](../s/setup_regexp_matches.md)
  - [build_regexp_split_result](../b/build_regexp_split_result.md)
  - [FuncCallContext](../F/FuncCallContext.md), regexp_matches_ctx, pg_re_flags
- Called from:
  - [regexp_split_to_table_no_flags](regexp_split_to_table_no_flags.md)

## Notes and Other Information
- Prohibits the 'g' (global) flag in user input but internally enables global matching
- Uses PostgreSQL's SRF (Set Returning Function) framework for returning multiple rows
- Maintains state across calls using regexp_matches_ctx stored in multi_call_memory_ctx
- Each call returns one split substring, with the function completing when all matches are processed
- Located at src/backend/utils/adt/regexp.c:1702-1754