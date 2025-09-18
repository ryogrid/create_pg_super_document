# PGTYPESdecimal_free

## Location
src/interfaces/ecpg/pgtypeslib/numeric.c: 392 - 406

## Overview
Deallocates memory used by a PostgreSQL decimal type structure in ECPG applications.

## Definition
```c
void PGTYPESdecimal_free(decimal *var)
```

## Detailed Description
This function performs simple memory deallocation for a decimal type structure. Unlike the numeric type, decimal structures do not have internal digit buffers that require separate cleanup, so this function only needs to free the structure itself. It is used for releasing memory allocated for decimal values in ECPG applications, particularly for Informix compatibility features.

## Parameters / Member Variables
- `var`: Pointer to the decimal structure to be freed

## Dependencies
- Functions called/Symbols referenced:
  - free (standard library function for memory deallocation)
  - decimal (type definition)
- Called from (representative examples):
  - Test functions (compat_informix-dec_test.c, pgtypeslib-num_test2.c)
  - ECPG precompiled code (preproc-outofscope.c, sql-sqlda.c)

## Notes and Other Information
- Simpler than PGTYPESnumeric_free as decimal structures don't have internal buffers
- Part of the ECPG pgtypes library for PostgreSQL embedded SQL
- Primarily used in Informix compatibility scenarios
- Should be called for every decimal value that was dynamically allocated
- Must not be called on static or stack-allocated decimal structures
- Located in src/interfaces/ecpg/pgtypeslib/numeric.c:392-406