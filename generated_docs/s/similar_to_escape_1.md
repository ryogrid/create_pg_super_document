# similar_to_escape_1

## Location
[src/backend/utils/adt/regexp.c:1048-1065](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regexp.c#L1048-L1065)

## Overview
A PostgreSQL SQL function wrapper that converts a SIMILAR TO pattern to POSIX-style regular expression format using the default escape character.

## Definition
```c
Datum similar_to_escape_1(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the PostgreSQL SQL function `similar_to_escape(pattern)` that takes a single text argument: a SIMILAR TO pattern. It serves as a thin wrapper around `similar_escape_internal()`, automatically using the default escape character (backslash '\') by passing NULL as the escape parameter.

The function is part of PostgreSQL's SQL standard compliance for the SIMILAR TO operator. When users don't need a custom escape character, this single-parameter version provides a convenient way to convert SIMILAR TO patterns to POSIX regular expressions that can be processed by PostgreSQL's regexp engine.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `pat_text` (arg 0): The SIMILAR TO pattern text to be converted

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_TEXT_PP` (macro for extracting text arguments)
  - [similar_escape_internal](similar_escape_internal.md) (performs the actual pattern conversion with NULL escape parameter)
  - `PG_RETURN_TEXT_P` (macro for returning text result)
- Called from:
  - SQL queries using `similar_to_escape(pattern)` function

## Notes and Other Information
- This is the 1-argument version of the similar_to_escape function family
- Automatically uses the default escape character (backslash '\') by passing NULL to `similar_escape_internal()`
- The function directly delegates all processing to `similar_escape_internal()`
- Located in `src/backend/utils/adt/regexp.c:1048-1065`
- The converted pattern includes anchors (^ and $) and non-capturing groups to ensure proper SQL SIMILAR TO semantics
- More convenient than the 2-argument version when custom escape characters are not needed