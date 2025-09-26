# ECPG_informix_set_var

## Location
[src/interfaces/ecpg/compatlib/informix.c:1019-1024](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/compatlib/informix.c#L1019-L1024)

## Overview
ECPG_informix_set_var is a wrapper function in PostgreSQL's ECPG Informix compatibility library that sets a variable in the ECPG environment.

## Definition
```c
void ECPG_informix_set_var(int number, void *pointer, int lineno)
```

## Detailed Description
This function serves as an Informix-compatible wrapper around the core ECPG function ECPGset_var. It's part of the ECPG (Embedded SQL in C for PostgreSQL) Informix compatibility layer, designed to provide API compatibility for applications originally written for Informix ESQL/C.

The function simply forwards all its parameters to ECPGset_var, which handles the actual variable setting functionality. This design pattern allows Informix-style code to work with PostgreSQL's ECPG system without modification, as the function signature and behavior match what Informix applications expect.

## Parameters / Member Variables
- `number`: An integer identifier for the variable to be set
- `pointer`: A void pointer to the variable's memory location
- `lineno`: An integer representing the line number in the source code (used for debugging and error reporting)

## Dependencies
- Functions called/Symbols referenced:
  - [ECPGset_var](ECPGset_var.md) (the core ECPG function that performs the actual variable setting)
- Called from (representative examples):
  - Referenced in ECPG_INFORMIX_EXTRA_CHARS macro at src/interfaces/ecpg/include/ecpg_informix.h:57

## Notes and Other Information
- This is a compatibility wrapper function that directly delegates to ECPGset_var
- Part of the ECPG Informix compatibility library
- Enables seamless migration of Informix ESQL/C applications to PostgreSQL
- Maintains the same function signature and behavior as the corresponding Informix function
- The lineno parameter is typically used for error reporting and debugging purposes