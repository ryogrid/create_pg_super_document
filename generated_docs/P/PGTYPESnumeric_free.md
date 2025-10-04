# PGTYPESnumeric_free

## Location
[src/interfaces/ecpg/pgtypeslib/numeric.c:385-391](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/pgtypeslib/numeric.c#L385-L391)

## Overview
Deallocates memory used by a PostgreSQL numeric type structure in ECPG applications.

## Definition
```c
void PGTYPESnumeric_free(numeric *var)
```

## Detailed Description
This function performs complete cleanup of a numeric type structure by first freeing the internal digit buffer memory and then freeing the numeric structure itself. It is the proper way to release memory allocated for numeric values in ECPG applications and should be called for every numeric value created with functions like PGTYPESnumeric_new() or PGTYPESnumeric_from_asc().

## Parameters / Member Variables
- `var`: Pointer to the numeric structure to be freed

## Dependencies
- Functions called/Symbols referenced:
  - digitbuf_free (for releasing internal digit buffer)
  - free (standard library function for memory deallocation)
  - [numeric](../n/numeric.md) (type definition)
- Called from (representative examples):
  - [PGTYPESnumeric_from_asc](PGTYPESnumeric_from_asc.md) (cleanup on error)
  - [PGTYPESnumeric_to_asc](PGTYPESnumeric_to_asc.md) (cleanup of temporary copy)
  - Various Informix compatibility functions (deccall2, deccall3, etc.)
  - ECPG data handling functions (ecpg_get_data, ecpg_store_input)
  - Test functions and user applications

## Notes and Other Information
- Essential for preventing memory leaks in ECPG applications
- Must be called for every numeric value that was dynamically allocated
- Follows the standard pattern: free internal resources first, then the structure
- Part of the ECPG pgtypes library for PostgreSQL embedded SQL
- Should not be called on static or stack-allocated numeric structures
- Located in src/interfaces/ecpg/pgtypeslib/numeric.c:385-391

## Simplified Source

```c
void PGTYPESnumeric_free(numeric *var) {
    // Free internal digit buffer
    digitbuf_free(var->buf);

    // Free the numeric structure itself
    free(var);
}
```