# string_to_text

## Location
[src/backend/utils/adt/ruleutils.c:13245-13257](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L13245-L13257)

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
  - [cstring_to_text](../c/cstring_to_text.md)
  - [pfree](../p/pfree.md)
- Called from (representative examples):
  - [pg_get_ruledef](../p/pg_get_ruledef.md)
  - [pg_get_viewdef](../p/pg_get_viewdef.md)
  - [pg_get_triggerdef](../p/pg_get_triggerdef.md)
  - [pg_get_indexdef](../p/pg_get_indexdef.md)
  - [pg_get_constraintdef](../p/pg_get_constraintdef.md)
  - [pg_get_functiondef](../p/pg_get_functiondef.md)

## Notes and Other Information
- This is a static function within ruleutils.c, primarily used for rule decompilation utilities
- The function assumes ownership of the input string and will free it, so callers should not use the string after passing it to this function
- Used extensively throughout the rule utility functions for converting temporary string representations to TEXT datums
- The memory management pattern (allocate, convert, free) is a common idiom in PostgreSQL for temporary string processing

## Simplified Source

```c
static text *string_to_text(char *str) {
    // Convert C string to PostgreSQL TEXT datum
    text *result = cstring_to_text(str);

    // Free the input string (assumes it was palloc'd)
    pfree(str);

    return result;
}
```