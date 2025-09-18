# AllocPointer

## Location
[src/backend/utils/mmgr/aset.c:113-121](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/aset.c#L113-L121)

## Overview
AllocPointer is a typedef that represents an aligned void pointer used for memory allocation within PostgreSQL's allocation set memory management system.

## Definition
```c
typedef void *AllocPointer;
```

## Detailed Description
AllocPointer is a simple typedef that wraps a void pointer to represent aligned memory pointers within the allocation set system. The typedef provides semantic clarity by indicating that the pointer is specifically used for memory allocation purposes and is guaranteed to be properly aligned. This abstraction helps distinguish allocation-related pointers from general-purpose void pointers in the codebase, making the memory management code more self-documenting and type-safe.

## Parameters / Member Variables
- This is a typedef for void*, so it has no member variables
- Represents: A generic pointer to allocated memory that is properly aligned

## Dependencies
- Functions called/Symbols referenced:
  - void* (standard C pointer type)
- Called from (representative examples):
  - [AllocSetRealloc](AllocSetRealloc.md) (uses AllocPointer for memory reallocation operations)

## Notes and Other Information
- Part of PostgreSQL's allocation set memory management system
- Provides type safety and semantic clarity for allocation-related pointers
- The 'aligned' aspect mentioned in comments indicates that pointers of this type follow proper memory alignment requirements
- Used sparingly in the codebase, primarily for internal memory management operations
- The typedef helps distinguish allocation system pointers from general void pointers