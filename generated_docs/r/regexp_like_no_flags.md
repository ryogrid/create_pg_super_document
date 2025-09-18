# regexp_like_no_flags

## Location
src/backend/utils/adt/regexp.c: 1311 - 1320

## Overview
A PostgreSQL wrapper function that provides the regexp_like functionality without requiring explicit flags parameter to maintain compatibility with the opr_sanity regression test.

## Definition
```c
Datum regexp_like_no_flags(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as a thin wrapper around the main `regexp_like` function. It was created specifically to keep the opr_sanity regression test from complaining about function parameter variations. The function simply forwards all its arguments to the main `regexp_like` implementation without any additional processing.

This variant provides the same boolean pattern matching functionality as `regexp_like` but with a simplified interface that doesn't require explicit specification of regex flags, making it easier to use for basic pattern matching scenarios where default regex behavior is sufficient.

## Parameters / Member Variables
- `fcinfo`: Standard PostgreSQL function call information structure containing all function arguments

## Dependencies
- Functions called/Symbols referenced:
  - regexp_like
- Called from (representative examples):
  - (No direct references found in the codebase)

## Notes and Other Information
- This function exists primarily for testing compatibility purposes
- It's a direct passthrough to the main regexp_like function
- The function signature follows PostgreSQL's V1 calling convention using PG_FUNCTION_ARGS
- Located in src/backend/utils/adt/regexp.c:1311-1320
- This is part of the family of wrapper functions that provide simplified interfaces to PostgreSQL's regex functionality
- Returns the same boolean result as regexp_like but without requiring flags parameter