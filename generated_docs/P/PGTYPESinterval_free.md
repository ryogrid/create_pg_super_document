# PGTYPESinterval_free

## Location
[src/interfaces/ecpg/pgtypeslib/interval.c:997-1002](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/pgtypeslib/interval.c#L997-L1002)

## Overview
Frees memory allocated for an interval data structure in the PostgreSQL ECPG pgtypes library.

## Definition
void PGTYPESinterval_free(interval *intvl)

## Detailed Description
This is a simple memory deallocation function that frees the memory pointed to by an interval pointer. It serves as a cleanup function for interval objects that were previously allocated using functions like PGTYPESinterval_new() or similar allocation routines. The function is part of the PostgreSQL ECPG (Embedded SQL in C) pgtypes library, which provides C data types corresponding to PostgreSQL server data types.

## Parameters / Member Variables
- intvl: Pointer to the interval structure to be freed. This should be a pointer that was previously allocated using malloc or a similar allocation function.

## Dependencies
- Functions called/Symbols referenced:
  - free (standard C library function)
  - interval (data type)
- Called from (representative examples):
  - [main](../m/main.md) (in test programs dt_test.c and dt_test2.c)
  - Various client applications using ECPG interval types

## Notes and Other Information
- This function should only be called on interval pointers that were dynamically allocated
- After calling this function, the pointer becomes invalid and should not be dereferenced
- This is a standard cleanup pattern in C programming for heap-allocated structures
- The function is declared in pgtypes_interval.h header file
- Part of the ECPG pgtypes library which provides client-side data type support for PostgreSQL