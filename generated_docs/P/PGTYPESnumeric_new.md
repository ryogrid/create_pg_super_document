# PGTYPESnumeric_new

## Location
[src/interfaces/ecpg/pgtypeslib/numeric.c:42-58](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/pgtypeslib/numeric.c#L42-L58)

## Overview
A constructor function that creates and initializes a new numeric value structure for use with PostgreSQL's ECPG (Embedded SQL in C) pgtypes library.

## Definition

```c
numeric *
PGTYPESnumeric_new(void)
```
## Detailed Description
The `PGTYPESnumeric_new` function serves as the primary constructor for creating new numeric values in PostgreSQL's ECPG pgtypes library. This function handles the complete initialization process by allocating memory for both the numeric structure itself and its internal digit buffer. It provides a clean interface for client applications to create numeric values without needing to understand the underlying memory management complexities. The function ensures proper error handling by checking allocation failures and cleaning up partial allocations if necessary.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [pgtypes_alloc](../p/pgtypes_alloc.md) (allocates memory for the numeric structure)
  - [alloc_var](../a/alloc_var.md) (allocates and initializes the digit buffer)
  - free (deallocates memory on error)
- Called from (representative examples):
  - [deccall2](../d/deccall2.md) (in Informix compatibility layer)
  - [deccall3](../d/deccall3.md) (in Informix compatibility layer)
  - [deccvdbl](../d/deccvdbl.md) (decimal conversion functions)
  - [ecpg_get_data](../e/ecpg_get_data.md) (ECPG data retrieval)
  - [ecpg_store_input](../e/ecpg_store_input.md) (ECPG input processing)
  - Various test programs

## Notes and Other Information
- Returns a pointer to a newly allocated numeric structure, or NULL if allocation fails
- The function initializes the numeric with zero digits, creating an essentially empty numeric ready for value assignment
- This is part of the ECPG pgtypes library, designed for client-side numeric operations in embedded SQL applications
- Proper error handling ensures no memory leaks occur during partial allocation failures
- The returned numeric should be freed using appropriate cleanup functions when no longer needed
- This function is widely used throughout the Informix compatibility layer and ECPG data handling routines