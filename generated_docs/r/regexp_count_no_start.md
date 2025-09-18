# regexp_count_no_start

## Location
src/backend/utils/adt/regexp.c: 1135 - 1141

## Overview
A PostgreSQL SQL function wrapper that provides a 3-argument interface to regexp_count without specifying the start parameter.

## Definition
```c
Datum regexp_count_no_start(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as a thin wrapper around the main `regexp_count()` function, providing an alternative function signature for cases where the start parameter is not needed. It directly delegates all processing to `regexp_count()` by passing the entire function call information (`fcinfo`) through.

The function exists primarily to satisfy PostgreSQL's function overloading system and to keep the opr_sanity regression test from complaining about function signature mismatches. It allows users to call regexp_count with 3 arguments (string, pattern, flags) without having to specify the start parameter, while still using the same underlying implementation.

This is a common pattern in PostgreSQL where multiple function signatures are needed for the same underlying functionality to provide user convenience and maintain SQL standard compliance.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure that is passed directly to `regexp_count()`, typically containing:
  - `str` (arg 0): The target string to search within
  - `pattern` (arg 1): The regular expression pattern to match
  - `flags` (arg 2): Regex flags string (e.g., 'i' for case-insensitive)

## Dependencies
- Functions called/Symbols referenced:
  - `[regexp_count](regexp_count.md)` (the main implementation function)
- Called from:
  - SQL queries using `regexp_count(string, pattern, flags)` function signature

## Notes and Other Information
- Located in `src/backend/utils/adt/regexp.c:1135-1141`
- Exists primarily for function overloading and regression test compatibility
- Provides no additional functionality beyond `regexp_count()`
- The comment indicates it's separated specifically to avoid opr_sanity regression test issues
- When this function is called, the start parameter in `regexp_count()` will default to 1
- Part of PostgreSQL's approach to providing multiple convenient function signatures for the same underlying functionality
- The function signature allows calling regexp_count without specifying a start position while still providing flags