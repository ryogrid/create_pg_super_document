# dectoint

## Location
[src/interfaces/ecpg/compatlib/informix.c:453-479](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/compatlib/informix.c#L453-L479)

## Overview
Converts a decimal type to an integer, providing Informix compatibility functionality in PostgreSQL's ECPG interface.

## Definition

```c
int
dectoint(decimal *np, int *ip)
```
## Detailed Description
The  function is part of PostgreSQL's ECPG (Embedded SQL in C) compatibility library for Informix. It converts a decimal value to an integer by first converting the decimal to PostgreSQL's internal numeric representation, then extracting the integer value. The function handles memory allocation, error checking, and proper cleanup of resources. It provides comprehensive error handling for out-of-memory conditions and numeric overflow scenarios.

## Parameters / Member Variables
- : Pointer to the input decimal value to be converted
- : Pointer to the integer variable where the converted result will be stored

## Dependencies
- Functions called/Symbols referenced:
  - [PGTYPESnumeric_new](../P/PGTYPESnumeric_new.md)
  - [PGTYPESnumeric_from_decimal](../P/PGTYPESnumeric_from_decimal.md)  
  - [PGTYPESnumeric_to_int](../P/PGTYPESnumeric_to_int.md)
  - [PGTYPESnumeric_free](../P/PGTYPESnumeric_free.md)
  - ECPG_INFORMIX_OUT_OF_MEMORY (error constant)
  - ECPG_INFORMIX_NUM_OVERFLOW (error constant)
  - PGTYPES_NUM_OVERFLOW (error constant)
- Called from (representative examples):
  - [main](../m/main.md) (in test programs)
  - ECPG_INFORMIX_EXTRA_CHARS (referenced in header)

## Notes and Other Information
- Returns 0 on success, error codes on failure
- Handles memory allocation failures gracefully
- Converts numeric overflow errors from PostgreSQL types to Informix-compatible error codes
- Part of the Informix compatibility layer in PostgreSQL ECPG
- Located in src/interfaces/ecpg/compatlib/informix.c:453-479
- Uses errno to detect overflow conditions in the underlying numeric conversion