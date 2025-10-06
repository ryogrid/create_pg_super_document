# regexp_like

## Location
[src/backend/utils/adt/regexp.c:1283-1310](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regexp.c#L1283-L1310)

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
  - [pg_re_flags](../p/pg_re_flags.md) (struct type)
  - [parse_re_flags](../p/parse_re_flags.md)
  - [RE_compile_and_execute](../R/RE_compile_and_execute.md)
  - PG_GET_COLLATION
- Called from (representative examples):
  - [regexp_like_no_flags](regexp_like_no_flags.md)

## Notes and Other Information
- Returns boolean result (true/false) indicating pattern match
- Prohibits the global ('g') flag option - raises error if specified
- Uses PostgreSQL's advanced regular expression engine with REG_ADVANCED flags
- Supports various regex flags: case sensitivity (i/c), newline handling (n/m/p/s/w), syntax modes (e/b/q/t/x)
- Located in src/backend/utils/adt/regexp.c:1283-1310
- Functionally similar to textregexeq/texticregexeq operators but with configurable flags
- Part of PostgreSQL's SQL standard regex function family

## Simplified Source

```c
/* Test for pattern match within a string */
Datum
regexp_like(PG_FUNCTION_ARGS)
{
    text *str = PG_GETARG_TEXT_PP(0);
    text *pattern = PG_GETARG_TEXT_PP(1);
    text *flags = PG_GETARG_TEXT_PP_IF_EXISTS(2);
    pg_re_flags re_flags;

    // Parse regex flags and validate
    parse_re_flags(&re_flags, flags);
    if (re_flags.glob) {
        ereport(ERROR, "regexp_like() does not support global option");
    }

    // Perform pattern matching and return boolean result
    PG_RETURN_BOOL(RE_compile_and_execute(pattern,
                                         VARDATA_ANY(str),
                                         VARSIZE_ANY_EXHDR(str),
                                         re_flags.cflags,
                                         PG_GET_COLLATION(),
                                         0, NULL));
}
```