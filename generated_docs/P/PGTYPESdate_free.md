# PGTYPESdate_free

## Location
[src/interfaces/ecpg/pgtypeslib/datetime.c:25-30](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/pgtypeslib/datetime.c#L25-L30)

## Overview
Deallocates memory for a date object that was previously allocated by PGTYPESdate_new or other date allocation functions in the PostgreSQL ECPG pgtypeslib.

## Definition
```c
void PGTYPESdate_free(date *d)
```

## Detailed Description
PGTYPESdate_free is a memory deallocation function that frees the memory occupied by a date object. This function is the counterpart to PGTYPESdate_new and should be called to prevent memory leaks when a date object is no longer needed. The function simply calls the standard library free() function to release the memory pointed to by the date pointer.

## Parameters / Member Variables
- `d`: Pointer to the date object to be freed. Can be NULL (standard free() behavior)

## Dependencies
- Functions called/Symbols referenced:
  - free (standard C library function)
  - date (type reference)
- Called from (representative examples):
  - [main](../m/main.md) (in test cases)
  - Various ECPG applications for memory cleanup

## Notes and Other Information
- This function should be called for every date object allocated with PGTYPESdate_new to prevent memory leaks
- It is safe to call this function with a NULL pointer (standard free() behavior)
- After calling this function, the pointer should not be used again unless it is reassigned
- Part of the ECPG pgtypeslib interface for managing PostgreSQL date types in C applications
- Located in src/interfaces/ecpg/pgtypeslib/datetime.c:25-30