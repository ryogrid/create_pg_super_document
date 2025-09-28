# MemoryContextStrdup

## Location
[src/backend/utils/mmgr/mcxt.c:1682-1694](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/mcxt.c#L1682-L1694)

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
  - [MemoryContextAlloc](MemoryContextAlloc.md) (allocates memory within the specified context)
  - strlen (C standard library function to get string length)
  - memcpy (C standard library function for memory copying)
- Called from (representative examples):
  - [init_string_reloption](../i/init_string_reloption.md) (relation option string initialization)
  - [DefineSavepoint](../D/DefineSavepoint.md) (savepoint name duplication)
  - [ExecuteQuery](../E/ExecuteQuery.md) (prepared statement query duplication)
  - [pstrdup](../p/pstrdup.md) (convenience wrapper for current memory context)
  - Various authentication and caching functions

## Notes and Other Information
- Returns a pointer to the newly allocated string copy
- The returned string will be freed when the memory context is reset or deleted
- Commonly used for storing configuration strings, identifiers, and cached values
- More efficient than manual allocation + strcpy combinations
- Widely used across PostgreSQL subsystems including authentication, caching, replication, and language extensions
- Located in src/backend/utils/mmgr/mcxt.c:1682-1694

## Simplified Source

```c
// Simplified version of MemoryContextStrdup
char *
MemoryContextStrdup(MemoryContext context, const char *string)
{
    // Calculate string length including null terminator
    Size len = strlen(string) + 1;

    // Allocate memory in the specified context
    char *new_string = (char *) MemoryContextAlloc(context, len);

    // Copy the entire string including null terminator
    memcpy(new_string, string, len);

    return new_string;
}
```

Key simplifications made:
- Used more descriptive variable names (`new_string` instead of `nstr`)
- Added explanatory comments for each core step
- Maintained the exact same logic flow and functionality
- Preserved all essential operations: length calculation, allocation, and copying