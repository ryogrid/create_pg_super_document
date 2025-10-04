# dectoasc

## Location
[src/interfaces/ecpg/compatlib/informix.c:381-431](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/compatlib/informix.c#L381-L431)

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
  - [PGTYPESnumeric_from_decimal](../P/PGTYPESnumeric_from_decimal.md)
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

## Simplified Source

```c
int dectoasc(decimal *np, char *cp, int len, int right)
{
    char *str;
    numeric *nres;

    // Handle null input
    rsetnull(CSTRINGTYPE, (char *) cp);
    if (risnull(CDECIMALTYPE, (char *) np))
        return 0;

    // Convert decimal to numeric for processing
    nres = PGTYPESnumeric_new();
    if (nres == NULL)
        return ECPG_INFORMIX_OUT_OF_MEMORY;

    if (PGTYPESnumeric_from_decimal(np, nres) != 0) {
        PGTYPESnumeric_free(nres);
        return ECPG_INFORMIX_OUT_OF_MEMORY;
    }

    // Convert to ASCII string with specified precision
    if (right >= 0)
        str = PGTYPESnumeric_to_asc(nres, right);
    else
        str = PGTYPESnumeric_to_asc(nres, nres->dscale);

    PGTYPESnumeric_free(nres);
    if (!str)
        return -1;

    // Check buffer length and copy result
    if ((int) (strlen(str) + 1) > len) {
        if (len > 1) {
            cp[0] = '*';  // Overflow indicator
            cp[1] = '\0';
        }
        free(str);
        return -1;
    } else {
        strcpy(cp, str);
        free(str);
        return 0;
    }
}
```