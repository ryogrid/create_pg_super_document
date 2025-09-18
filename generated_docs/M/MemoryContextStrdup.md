# MemoryContextStrdup

## Location
src/backend/utils/mmgr/mcxt.c: 1682 - 1694

## Overview
MemoryContextStrdup is a PostgreSQL memory management function that duplicates a C string within a specified memory context, similar to the standard strdup() function but using PostgreSQL's memory context system.

## Definition
```c
char *MemoryContextStrdup(MemoryContext context, const char *string)
```

## Detailed Description
This function provides a memory context-aware version of the standard C library strdup() function. It calculates the length of the input string, allocates appropriate memory within the specified context, and copies the entire string including the null terminator. The function is widely used throughout PostgreSQL for creating persistent copies of strings that need to live within specific memory contexts, ensuring proper memory management and cleanup when the context is reset or deleted.

## Parameters / Member Variables
- `context`: The memory context in which to allocate the duplicated string
- `string`: The null-terminated C string to duplicate

## Dependencies
- Functions called/Symbols referenced:
  - MemoryContextAlloc (allocates memory within the specified context)
  - strlen (C standard library function to get string length)
  - memcpy (C standard library function for memory copying)
- Called from (representative examples):
  - init_string_reloption (relation option string initialization)
  - DefineSavepoint (savepoint name duplication)
  - ExecuteQuery (prepared statement query duplication)
  - pstrdup (convenience wrapper for current memory context)
  - Various authentication and caching functions

## Notes and Other Information
- Returns a pointer to the newly allocated string copy
- The returned string will be freed when the memory context is reset or deleted
- Commonly used for storing configuration strings, identifiers, and cached values
- More efficient than manual allocation + strcpy combinations
- Widely used across PostgreSQL subsystems including authentication, caching, replication, and language extensions
- Located in src/backend/utils/mmgr/mcxt.c:1682-1694