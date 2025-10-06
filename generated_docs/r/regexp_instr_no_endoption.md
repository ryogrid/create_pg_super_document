# regexp_instr_no_endoption

## Location
[src/backend/utils/adt/regexp.c:1259-1265](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regexp.c#L1259-L1265)

## Overview
A wrapper function for regexp_instr that provides compatibility for function calls without the endoption parameter, kept separate to avoid opr_sanity regression test complaints.

## Definition
```c
Datum regexp_instr_no_endoption(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as a simple wrapper around the main regexp_instr function. It exists to handle PostgreSQL function overload resolution for calls that don't specify the endoption parameter. The function directly forwards all arguments to regexp_instr without any modification, allowing the main function to use its default value (endoption = 0) to return the starting position of matches rather than the ending position.

The comment indicates this separation is specifically to keep the opr_sanity regression test from complaining, which suggests this is part of PostgreSQL's internal function organization strategy for managing different function signatures in the SQL function dispatch system.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function call information structure containing all arguments passed from SQL (typically string, pattern, start, n, and optionally flags, subexpr)

## Dependencies
- Functions called/Symbols referenced:
  - [regexp_instr](regexp_instr.md): The main function that performs the actual regular expression position finding logic

- Called from (representative examples):
  - SQL function dispatcher (no direct C code references found)

## Notes and Other Information
- This is a thin wrapper function with no independent logic
- The separation exists purely for PostgreSQL's internal function organization and testing framework compatibility
- All actual functionality is implemented in the regexp_instr function
- Handles the common case where users want the starting position of matches (default endoption = 0) rather than ending positions
- Part of PostgreSQL's regular expression support in the backend utilities
- The endoption parameter in the main function controls whether to return the start (0) or end (1) position of the match
- Works with other optional parameters like start position, occurrence number, flags, and subexpression selection

## Simplified Source

```c
/* Wrapper for regexp_instr without endoption parameter */
Datum
regexp_instr_no_endoption(PG_FUNCTION_ARGS)
{
    return regexp_instr(fcinfo);
}
```