# PGTYPESdate_new

## Location
[src/interfaces/ecpg/pgtypeslib/datetime.c:15-24](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/pgtypeslib/datetime.c#L15-L24)

## Overview
Allocates memory for a new date object and returns a pointer to it for use in PostgreSQL ECPG date handling operations.

## Definition
```c
date *PGTYPESdate_new(void)
```

## Detailed Description
PGTYPESdate_new is a memory allocation function specifically designed for creating new date objects in the PostgreSQL ECPG (Embedded SQL in C) pgtypeslib. The function allocates memory for a single date structure using the pgtypes_alloc function, which is the standard memory allocator used throughout the pgtypes library. The function handles potential memory allocation failures gracefully by allowing a NULL return value when memory is unavailable.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [pgtypes_alloc](../p/pgtypes_alloc.md)
  - date (type reference)
- Called from (representative examples):
  - [main](../m/main.md) (in test cases)
  - Various ECPG applications requiring date object allocation

## Notes and Other Information
- The function can return NULL if memory allocation fails, so callers must check the return value
- Memory allocated by this function should be freed using PGTYPESdate_free to prevent memory leaks
- This is part of the ECPG pgtypeslib interface for handling PostgreSQL date types in C applications
- Located in src/interfaces/ecpg/pgtypeslib/datetime.c:15-24