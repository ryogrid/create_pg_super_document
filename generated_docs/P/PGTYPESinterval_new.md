# PGTYPESinterval_new

## Location
src/interfaces/ecpg/pgtypeslib/interval.c: 987 - 996

## Overview
PGTYPESinterval_new is a public API function that allocates and returns a new interval structure for use in ECPG client applications.

## Definition


## Detailed Description
This function provides a standard way to allocate memory for a new interval structure in client applications using the ECPG (Embedded C for PostgreSQL) interface. It uses the pgtypes memory allocation system to ensure proper memory management within the PostgreSQL type system context.

The function performs a simple memory allocation for an interval structure and returns the pointer to the caller. The allocated memory is uninitialized, so callers are responsible for properly initializing the interval fields before use. The function may return NULL if memory allocation fails, which callers should check.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - interval (interval data structure type)
  - [pgtypes_alloc](../p/pgtypes_alloc.md) (PostgreSQL types memory allocation function)
- Called from (representative examples):
  - Various ECPG client applications and test programs
  - Referenced in pgtypes_interval.h header file
  - Used in array and datetime test programs

## Notes and Other Information
- Located in src/interfaces/ecpg/pgtypeslib/interval.c:987-996
- Part of the public PGTYPES API for ECPG client applications
- Returns NULL if memory allocation fails - callers must check the return value
- The allocated interval structure is uninitialized and must be set up by the caller
- Memory allocated by this function should be freed using appropriate pgtypes deallocation functions
- Provides a clean abstraction for interval memory management in embedded C programs
- Used in conjunction with other PGTYPESinterval_* functions for complete interval handling