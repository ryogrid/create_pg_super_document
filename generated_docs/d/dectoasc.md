# dectoasc

## Location
src/interfaces/ecpg/compatlib/informix.c: 381 - 431

## Overview
Converts a decimal number to its ASCII string representation using ECPG Informix compatibility library.

## Definition
```c
int dectoasc(decimal *np, char *cp, int len, int right)
```

## Detailed Description
The `dectoasc` function converts a decimal number to its ASCII string representation with specified formatting options. This function is part of PostgreSQL's ECPG (Embedded SQL in C) Informix compatibility library, providing compatibility with Informix database decimal-to-string conversion operations. The function handles null values appropriately, manages memory allocation for intermediate numeric operations, and includes buffer overflow protection. It uses the PostgreSQL numeric type system internally for the conversion process.

## Parameters / Member Variables
- `np`: Pointer to the decimal number to be converted
- `cp`: Pointer to the character buffer where the ASCII result will be stored
- `len`: Maximum length of the output buffer (including null terminator)
- `right`: Number of digits to display after the decimal point (if >= 0), or use the decimal's natural scale (if < 0)

## Dependencies
- Functions called/Symbols referenced:
  - [rsetnull](../r/rsetnull.md)
  - [risnull](../r/risnull.md)
  - [PGTYPESnumeric_new](../P/PGTYPESnumeric_new.md)
  - PGTYPESnumeric_from_decimal
  - [PGTYPESnumeric_to_asc](../P/PGTYPESnumeric_to_asc.md)
  - [PGTYPESnumeric_free](../P/PGTYPESnumeric_free.md)
  - strlen
  - strcpy
  - free
- Called from (representative examples):
  - [main](../m/main.md) (in test files)
  - [dump_sqlda](dump_sqlda.md)
- Type constants used:
  - CSTRINGTYPE
  - CDECIMALTYPE
- Error constants used:
  - ECPG_INFORMIX_OUT_OF_MEMORY

## Notes and Other Information
- Returns 0 on success
- Returns ECPG_INFORMIX_OUT_OF_MEMORY when memory allocation fails
- Returns -1 when the result string is too long for the buffer
- Handles null decimal values by setting the output string to null
- When buffer overflow occurs and len > 1, sets cp[0] to '*' and cp[1] to '\0'
- Contains a TODO comment about implementing exponential notation for very long numbers
- Located in src/interfaces/ecpg/compatlib/informix.c:381-431
- Performs proper memory cleanup of intermediate numeric values