# pstrdup

## Location
[src/backend/utils/mmgr/mcxt.c:1695-1705](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/mcxt.c#L1695-L1705)

## Overview
A utility function that duplicates a null-terminated string using the current memory context, providing a convenient wrapper around MemoryContextStrdup.

## Definition

```c
char *
pstrdup(const char *in)
```
## Detailed Description
pstrdup is a simple wrapper function that creates a duplicate copy of a null-terminated string in the current memory context. It uses the PostgreSQL memory management system to allocate memory for the new string copy. This function is commonly used throughout PostgreSQL when a persistent copy of a string is needed that will be automatically freed when the current memory context is reset or deleted.

The function relies on MemoryContextStrdup to perform the actual work, passing the global CurrentMemoryContext as the target memory context for allocation.

## Parameters / Member Variables
- `in`: The null-terminated input string to be duplicated. Must not be NULL.

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextStrdup](../M/MemoryContextStrdup.md)
  - CurrentMemoryContext (global variable)
- Called from (representative examples):
  - Various PostgreSQL functions that need string duplication (no direct references found in symbol analysis)

## Notes and Other Information
- This is a convenience function that simplifies string duplication by using the current memory context
- The returned string will be automatically freed when the current memory context is reset or deleted
- Located in src/backend/utils/mmgr/mcxt.c at lines 1695-1705
- Part of PostgreSQL's memory management subsystem
- Should be used instead of standard C library strdup() to ensure proper memory context management

## Simplified Source

```c
char *
pstrdup(const char *in)
{
    // Duplicate string in current memory context
    // Memory will be automatically freed when context is reset/deleted
    return MemoryContextStrdup(CurrentMemoryContext, in);
}
```