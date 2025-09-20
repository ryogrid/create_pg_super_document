# rtypmsize

## Location
[src/interfaces/ecpg/compatlib/informix.c:1003-1010](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/compatlib/informix.c#L1003-L1010)

## Overview
rtypmsize is a stub function in PostgreSQL's ECPG Informix compatibility library that returns the memory size requirement for a given type.

## Definition

```c
struct sqlca_t *sqlca = ECPGget_sqlca();
```
## Detailed Description
This function is part of the ECPG (Embedded SQL in C for PostgreSQL) Informix compatibility layer. It's designed to return the memory size in bytes required for a particular data type and length combination. However, in the current implementation, it's a stub function that simply returns 0 regardless of the input parameters. The function parameters are explicitly cast to void to suppress compiler warnings about unused parameters.

This stub implementation suggests that either:
1. The functionality is not fully implemented in PostgreSQL's Informix compatibility layer
2. The function is provided for API compatibility but not actually used in typical operations
3. It may be intended for future implementation

## Parameters / Member Variables
- : An integer representing the data type identifier
- : An integer representing the length or size parameter for the type

## Dependencies
- Functions called/Symbols referenced: None
- Called from (representative examples):
  - Referenced in ECPG_INFORMIX_EXTRA_CHARS macro at src/interfaces/ecpg/include/ecpg_informix.h:50

## Notes and Other Information
- This is a stub function that always returns 0
- Part of the ECPG Informix compatibility library
- Parameters are explicitly unused to avoid compiler warnings
- May be intended for future implementation or provided for API compatibility