# regexp_like

## Location
src/backend/utils/adt/regexp.c: 1283 - 1310

## Overview
Tests for a pattern match within a string using regular expressions, providing boolean result indicating whether the pattern matches the input text.

## Definition
```c
Datum regexp_like(PG_FUNCTION_ARGS)
```

## Detailed Description
The `regexp_like` function implements pattern matching functionality that tests whether a regular expression pattern matches anywhere within a given string. It returns a boolean value (true if the pattern is found, false otherwise). This function is similar to the LIKE operator but uses regular expression syntax for more powerful pattern matching capabilities.

The function accepts optional flags to control regex behavior such as case sensitivity, newline handling, and other regex compilation options. However, it explicitly prohibits the use of the global ('g') flag since it only needs to determine if a match exists, not find all matches.

Internally, the function uses PostgreSQL's regular expression engine (`RE_compile_and_execute`) to perform the pattern matching operation.

## Parameters / Member Variables
- `str`: The input text string to be searched (PG_GETARG_TEXT_PP(0))
- `pattern`: The regular expression pattern to match against (PG_GETARG_TEXT_PP(1))
- `flags`: Optional text parameter containing regex flags (PG_GETARG_TEXT_PP_IF_EXISTS(2))

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TEXT_PP_IF_EXISTS
  - pg_re_flags (struct type)
  - parse_re_flags
  - RE_compile_and_execute
  - PG_GET_COLLATION
- Called from (representative examples):
  - regexp_like_no_flags

## Notes and Other Information
- Returns boolean result (true/false) indicating pattern match
- Prohibits the global ('g') flag option - raises error if specified
- Uses PostgreSQL's advanced regular expression engine with REG_ADVANCED flags
- Supports various regex flags: case sensitivity (i/c), newline handling (n/m/p/s/w), syntax modes (e/b/q/t/x)
- Located in src/backend/utils/adt/regexp.c:1283-1310
- Functionally similar to textregexeq/texticregexeq operators but with configurable flags
- Part of PostgreSQL's SQL standard regex function family