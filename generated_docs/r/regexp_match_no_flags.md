# regexp_match_no_flags

## Location
src/backend/utils/adt/regexp.c: 1357 - 1366

## Overview
A wrapper function for regexp_match that provides a separate entry point without flags to satisfy the opr_sanity regression test requirements.

## Definition


## Detailed Description
This function serves as a simple wrapper around the main regexp_match function. It exists primarily to keep the opr_sanity regression test from complaining about the function signature or implementation. The function directly delegates all processing to regexp_match by passing through the function call information structure.

## Parameters / Member Variables
- Uses PostgreSQL's standard function call interface (PG_FUNCTION_ARGS)
- No direct parameters - receives arguments through the fcinfo structure

## Dependencies
- Functions called/Symbols referenced:
  - regexp_match
- Called from (representative examples):
  - No direct callers found in the codebase

## Notes and Other Information
- This is a compatibility wrapper function specifically created to satisfy regression test requirements
- The function is located in src/backend/utils/adt/regexp.c at lines 1357-1366
- All actual regular expression matching logic is handled by the regexp_match function