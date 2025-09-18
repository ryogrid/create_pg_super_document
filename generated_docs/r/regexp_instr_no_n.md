# regexp_instr_no_n

## Location
src/backend/utils/adt/regexp.c: 1252 - 1258

## Overview
A wrapper function for regexp_instr that provides compatibility for function calls without the occurrence number (n) parameter, kept separate to avoid opr_sanity regression test complaints.

## Definition
```c
Datum regexp_instr_no_n(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as a simple wrapper around the main regexp_instr function. It exists to handle PostgreSQL function overload resolution for calls that don't specify the occurrence number parameter (n). The function directly forwards all arguments to regexp_instr without any modification, allowing the main function to use its default value (n = 1) to find the first occurrence of the pattern match.

The comment indicates this separation is specifically to keep the opr_sanity regression test from complaining, which suggests this is part of PostgreSQL's internal function organization strategy for managing different function signatures in the SQL function dispatch system.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function call information structure containing all arguments passed from SQL (typically string, pattern, start, and optionally endoption, flags, subexpr)

## Dependencies
- Functions called/Symbols referenced:
  - [regexp_instr](regexp_instr.md): The main function that performs the actual regular expression position finding logic

- Called from (representative examples):
  - SQL function dispatcher (no direct C code references found)

## Notes and Other Information
- This is a thin wrapper function with no independent logic
- The separation exists purely for PostgreSQL's internal function organization and testing framework compatibility
- All actual functionality is implemented in the regexp_instr function
- Handles the common case where users want to find the first occurrence of a pattern match (default n = 1)
- Part of PostgreSQL's regular expression support in the backend utilities
- Works with other optional parameters like start position, endoption, flags, and subexpression selection