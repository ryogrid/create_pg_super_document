# nameregexeq

## Location
[src/backend/utils/adt/regexp.c:459-472](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regexp.c#L459-L472)

## Overview
The nameregexeq function performs regular expression matching on PostgreSQL name data types, returning true if the name matches the provided regular expression pattern.

## Definition

```c
Datum
nameregexeq(PG_FUNCTION_ARGS)
```
## Detailed Description
This function is a PostgreSQL interface routine called by the function manager to implement the ~= operator for name data types. It takes a PostgreSQL name (a fixed-length string type used for identifiers) and a text pattern containing a regular expression, then determines if the name matches the pattern. The function uses the RE_compile_and_execute utility with advanced regular expression features enabled and respects the current collation settings for locale-aware matching.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - Argument 0: Name value to be tested against the pattern
  - Argument 1: Text containing the regular expression pattern

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_NAME: Extracts name argument from function arguments
  - PG_GETARG_TEXT_PP: Extracts text argument (pattern) from function arguments
  - [RE_compile_and_execute](../R/RE_compile_and_execute.md): Compiles and executes the regular expression
  - NameStr: Converts Name to null-terminated string
  - strlen: Calculates string length
  - PG_GET_COLLATION: Gets current collation for locale-aware matching
  - PG_RETURN_BOOL: Returns boolean result to PostgreSQL
  - REG_ADVANCED: Flag for advanced regular expression features
- Called from (representative examples):
  - No direct callers found (typically invoked through PostgreSQL operator system)

## Notes and Other Information
- This function implements the ~= operator for name types in PostgreSQL SQL queries
- Uses REG_ADVANCED flag to enable advanced regular expression features
- Respects collation settings for proper locale-aware string matching
- The function is part of PostgreSQL's regular expression infrastructure in src/backend/utils/adt/regexp.c
- Returns a Datum containing a boolean value indicating match success