# texticregexne

## Location
[src/backend/utils/adt/regexp.c:564-582](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regexp.c#L564-L582)

## Overview
A PostgreSQL function that performs case-insensitive regular expression matching for text values, returning true if the pattern does NOT match the text.

## Definition
```c
Datum texticregexne(PG_FUNCTION_ARGS)
```

## Detailed Description
The `texticregexne` function implements the SQL operator `!~*` for PostgreSQL's text data type. It takes a text value and a regular expression pattern, performs case-insensitive pattern matching, and returns the negation of the match result. The function uses PostgreSQL's advanced regular expression engine with case-insensitive flags to evaluate whether the given pattern does NOT match anywhere within the text string.

## Parameters / Member Variables
- `PG_GETARG_TEXT_PP(0)`: The text value to be tested against the regular expression pattern
- `PG_GETARG_TEXT_PP(1)`: The regular expression pattern as a text value

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_TEXT_PP`: Extracts text argument from function call with potential detoasting
  - [RE_compile_and_execute](../R/RE_compile_and_execute.md): Core regex compilation and execution function
  - `VARDATA_ANY`: Gets pointer to the actual data portion of a text variable
  - `VARSIZE_ANY_EXHDR`: Gets the size of text data excluding the header
  - `PG_GET_COLLATION`: Gets collation information for the operation
  - `REG_ADVANCED`: Flag for advanced regular expression features
  - `REG_ICASE`: Flag for case-insensitive matching
- Called from (representative examples):
  - No direct references found in the codebase (likely called through SQL operator dispatch)

## Notes and Other Information
- This function implements the negated case-insensitive regex match operator (!~*)
- Uses PostgreSQL's advanced regex engine with case-insensitive matching
- Returns the boolean negation of the regex match result
- Handles variable-length text data with proper detoasting support
- Part of PostgreSQL's comprehensive set of regular expression operators for different data types
- The function is typically invoked through SQL expressions rather than direct function calls