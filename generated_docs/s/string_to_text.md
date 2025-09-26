# string_to_text

## Location
src/backend/utils/adt/ruleutils.c: 13245 - 13257

## Overview
A static utility function that converts a C string to a PostgreSQL TEXT datum, automatically freeing the input string memory.

## Definition
```c
static text *
string_to_text(char *str)
```

## Detailed Description
This function serves as a convenient wrapper around `cstring_to_text()` for scenarios where the input C string should be freed after conversion. It assumes that the input string was allocated using `palloc()` and automatically calls `pfree()` to release the memory after creating the TEXT datum. This pattern is commonly used in PostgreSQLs rule utility functions where temporary strings are created during rule decompilation and need to be cleaned up.

## Parameters / Member Variables
- `str`: A null-terminated C string that was allocated using `palloc()`. The function takes ownership of this memory and frees it.

## Dependencies
- Functions called/Symbols referenced:
  - cstring_to_text
  - pfree
- Called from (representative examples):
  - pg_get_ruledef
  - pg_get_viewdef
  - pg_get_triggerdef
  - pg_get_indexdef
  - pg_get_constraintdef
  - pg_get_functiondef

## Notes and Other Information
- This is a static function within ruleutils.c, primarily used for rule decompilation utilities
- The function assumes ownership of the input string and will free it, so callers should not use the string after passing it to this function
- Used extensively throughout the rule utility functions for converting temporary string representations to TEXT datums
- The memory management pattern (allocate, convert, free) is a common idiom in PostgreSQL for temporary string processing