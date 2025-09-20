# regexp_match_no_flags

## Location
[src/backend/utils/adt/regexp.c:1357-1366](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regexp.c#L1357-L1366)

## Overview
A wrapper function for regexp_match that provides a separate entry point without flags to satisfy the opr_sanity regression test requirements.

## Definition

```c
Datum
regexp_match_no_flags(PG_FUNCTION_ARGS)
```
## Detailed Description
This function serves as a simple wrapper around the main regexp_match function. It exists primarily to keep the opr_sanity regression test from complaining about the function signature or implementation. The function directly delegates all processing to regexp_match by passing through the function call information structure.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [regexp_match](regexp_match.md)
- Called from (representative examples):
  - No direct callers found in the codebase

## Notes and Other Information
- This is a compatibility wrapper function specifically created to satisfy regression test requirements
- The function is located in src/backend/utils/adt/regexp.c at lines 1357-1366
- All actual regular expression matching logic is handled by the regexp_match function