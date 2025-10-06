# rtrim1

## Location
[src/backend/utils/adt/oracle_compat.c:766-796](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/oracle_compat.c#L766-L796)

## Overview
A simplified right-trim function that removes trailing whitespace characters (spaces only) from the right side of a text string.

## Definition

```c
Datum
rtrim1(PG_FUNCTION_ARGS)
```
## Detailed Description
The rtrim1 function provides a streamlined version of PostgreSQL's rtrim functionality with a fixed character set of just space characters (' '). It serves as a wrapper around the internal dotrim function, specifically configured to trim only trailing spaces from text input. This function is part of PostgreSQL's Oracle compatibility layer, providing behavior similar to Oracle's RTRIM function when called without specifying a trim set.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: The input text string from which trailing spaces will be removed
## Dependencies
- Functions called/Symbols referenced:
  - [dotrim](../d/dotrim.md)
  - PG_RETURN_TEXT_P
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- The function is located in src/backend/utils/adt/oracle_compat.c:766-796
- This is a PostgreSQL built-in function designed for Oracle compatibility
- Unlike the generic rtrim function, this version has a hardcoded trim set of single space character
- Uses the internal dotrim function with parameters (string_data, string_length, " ", 1, false, true)
- The last two boolean parameters to dotrim indicate: left_trim=false, right_trim=true

## Simplified Source

```c
Datum
rtrim1(PG_FUNCTION_ARGS)
{
    // Extract input string argument
    text *string = PG_GETARG_TEXT_PP(0);
    text *result;

    // Trim trailing spaces using fixed " " character set
    // false=no left trim, true=right trim enabled
    result = dotrim(VARDATA_ANY(string), VARSIZE_ANY_EXHDR(string),
                    " ", 1,
                    false, true);

    PG_RETURN_TEXT_P(result);
}
```