# cache_single_string

## Location
[src/backend/utils/adt/pg_locale.c:806-828](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pg_locale.c#L806-L828)

## Overview
A utility function that converts and caches a single string from a specified encoding to the database encoding, managing memory allocation and cleanup for long-lived storage.

## Definition

```c
struct tm  *timeinfo;
```
## Detailed Description
The  function serves as a subroutine for  and handles the conversion and caching of locale-specific strings. It performs encoding conversion from the specified source encoding to PostgreSQL's database encoding, then stores the result in long-lived memory (TopMemoryContext) while properly managing memory cleanup.

The function ensures that:
1. The source string is converted to the database encoding or validated as compatible
2. The converted string is stored in persistent memory that survives beyond the current transaction
3. Any previously cached value is properly freed to prevent memory leaks
4. Temporary conversion results are cleaned up

This is essential for locale data that needs to persist across multiple database operations and must be in the correct encoding for the server.

## Parameters / Member Variables
- : Pointer to a char pointer where the converted string will be stored (replaces any existing value)
- : Source string to be converted from the specified encoding
- : Source encoding identifier (PostgreSQL encoding ID) of the input string

## Dependencies
- Functions called/Symbols referenced:
  -  (PostgreSQL encoding conversion)
  -  (C standard library)
  -  (PostgreSQL memory management)
  -  (PostgreSQL global memory context)
  -  (PostgreSQL memory deallocation)
- Called from (representative examples):
  -  (multiple times for different locale strings)

## Notes and Other Information
- This function is designed for caching locale-specific strings that need to persist beyond transaction boundaries
- Uses TopMemoryContext to ensure the strings survive for the lifetime of the backend process
- Properly handles memory management by freeing both old cached values and temporary conversion results
- The function is static and only used within the pg_locale.c file
- Critical for ensuring locale data is in the correct encoding for database operations
- Handles the case where no conversion is needed (when ptr == src) efficiently

## Simplified Source

```c
static void
cache_single_string(char **dst, const char *src, int encoding)
{
    char *ptr;
    char *olddst;

    // Convert string to database encoding
    ptr = pg_any_to_server(src, strlen(src), encoding);

    // Store in long-lived memory, replacing any previous value
    olddst = *dst;
    *dst = MemoryContextStrdup(TopMemoryContext, ptr);

    // Clean up old cached value
    if (olddst)
        pfree(olddst);

    // Clean up temporary conversion result if needed
    if (ptr != src)
        pfree(ptr);
}
```