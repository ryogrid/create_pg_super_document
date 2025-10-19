# ECPG_informix_get_var

## Location
[src/interfaces/ecpg/compatlib/informix.c:1025-1030](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/compatlib/informix.c#L1025-L1030)

## Overview
ECPG_informix_get_var is a wrapper function in PostgreSQL's ECPG Informix compatibility library that retrieves a variable from the ECPG environment.

## Definition
```c
void *ECPG_informix_get_var(int number)
```

## Detailed Description
This function serves as an Informix-compatible wrapper around the core ECPG function ECPGget_var. It's part of the ECPG (Embedded SQL in C for PostgreSQL) Informix compatibility layer, designed to provide API compatibility for applications originally written for Informix ESQL/C.

The function simply forwards the number parameter to ECPGget_var and returns the result directly. This design pattern allows Informix-style code to work with PostgreSQL's ECPG system without modification, as the function signature and behavior match what Informix applications expect.

This function is the counterpart to ECPG_informix_set_var, providing the getter functionality for variables that have been previously set in the ECPG environment.

## Parameters / Member Variables
- `number`: An integer identifier for the variable to be retrieved

## Dependencies
- Functions called/Symbols referenced:
  - [ECPGget_var](ECPGget_var.md) (the core ECPG function that performs the actual variable retrieval)
- Called from (representative examples):
  - Referenced in ECPG_INFORMIX_EXTRA_CHARS macro at src/interfaces/ecpg/include/ecpg_informix.h:58

## Notes and Other Information
- This is a compatibility wrapper function that directly delegates to ECPGget_var
- Part of the ECPG Informix compatibility library
- Returns a void pointer to the variable's memory location
- Enables seamless migration of Informix ESQL/C applications to PostgreSQL
- Maintains the same function signature and behavior as the corresponding Informix function
- Works in conjunction with ECPG_informix_set_var for complete variable management compatibility

## Simplified Source
```c
void *ECPG_informix_get_var(int number) {
    // Informix compatibility wrapper - delegates to core ECPG function
    return ECPGget_var(number);
}
```