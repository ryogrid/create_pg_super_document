# regexp_instr_no_start

## Location
[src/backend/utils/adt/regexp.c:1245-1251](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regexp.c#L1245-L1251)

## Overview
A wrapper function for regexp_instr that provides compatibility for function calls without the start position parameter, kept separate to avoid opr_sanity regression test complaints.

## Definition
```c
Datum regexp_instr_no_start(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as a simple wrapper around the main regexp_instr function. It exists to handle PostgreSQL function overload resolution for calls that don't specify the starting position parameter. The function directly forwards all arguments to regexp_instr without any modification, allowing the main function to use its default value (start = 1) for the missing parameter.

The comment indicates this separation is specifically to keep the opr_sanity regression test from complaining, which suggests this is part of PostgreSQL's internal function organization strategy for managing different function signatures in the SQL function dispatch system.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function call information structure containing all arguments passed from SQL (typically string, pattern, and optionally n, endoption, flags, subexpr)

## Dependencies
- Functions called/Symbols referenced:
  - [regexp_instr](regexp_instr.md): The main function that performs the actual regular expression position finding logic

- Called from (representative examples):
  - SQL function dispatcher (no direct C code references found)

## Notes and Other Information
- This is a thin wrapper function with no independent logic
- The separation exists purely for PostgreSQL's internal function organization and testing framework compatibility
- All actual functionality is implemented in the regexp_instr function
- Handles the common case where users want to search from the beginning of the string (default start = 1)
- Part of PostgreSQL's regular expression support in the backend utilities

## Simplified Source

```c
/* Wrapper for regexp_instr without start parameter */
Datum
regexp_instr_no_start(PG_FUNCTION_ARGS)
{
    return regexp_instr(fcinfo);
}
```