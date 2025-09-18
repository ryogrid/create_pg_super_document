# repalloc0

## Location
[src/backend/utils/mmgr/mcxt.c:1618-1638](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/mcxt.c#L1618-L1638)

## Overview
repalloc0 is a PostgreSQL memory management function that adjusts the size of a previously allocated memory chunk and zeros out the newly added space.

## Definition
```c
void *repalloc0(void *pointer, Size oldsize, Size size)
```

## Detailed Description
This function extends the functionality of standard realloc by ensuring that any newly allocated space is initialized to zero. It serves as a safer alternative to repalloc when the caller needs to ensure that expanded memory regions are clean. The function validates that the new size is not smaller than the old size, throwing an error if invalid parameters are provided. After performing the reallocation through the standard repalloc function, it uses memset to zero-initialize the added space.

## Parameters / Member Variables
- `pointer`: Pointer to the previously allocated memory chunk to be resized  
- `oldsize`: Current size of the memory chunk being reallocated
- `size`: New desired size for the memory chunk (must be >= oldsize)

## Dependencies
- Functions called/Symbols referenced:
  - [repalloc](repalloc.md) (standard PostgreSQL realloc function)
  - memset (C standard library function for memory initialization)
  - elog (PostgreSQL error logging function)
- Called from (representative examples):
  - MCXT_ALLOC_ZERO (memory allocation macro with zero initialization)
  - repalloc0_array (array-specific zero-initialized reallocation)

## Notes and Other Information
- Validates argument order by checking that new size is not smaller than old size
- Throws ERROR-level exception for invalid size parameters
- Only zeros the newly added space, not the entire allocated area
- Commonly used for dynamic arrays and structures where clean initialization is required
- Located in src/backend/utils/mmgr/mcxt.c:1618-1638