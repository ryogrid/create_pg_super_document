# dectolong

## Location
[src/interfaces/ecpg/compatlib/informix.c:480-507](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/compatlib/informix.c#L480-L507)

## Overview
Converts a decimal type to a long integer, providing Informix compatibility functionality in PostgreSQL's ECPG interface.

## Definition
```c
int dectolong(decimal *np, long *lngp)
```

## Detailed Description
The `dectolong` function is part of PostgreSQL's ECPG (Embedded SQL in C) compatibility library for Informix. It converts a decimal value to a long integer by first converting the decimal to PostgreSQL's internal numeric representation, then extracting the long integer value. Similar to `dectoint`, this function handles memory allocation, error checking, and proper cleanup of resources, but targets the larger long integer data type instead of int.

## Parameters / Member Variables
- `np`: Pointer to the input decimal value to be converted
- `lngp`: Pointer to the long integer variable where the converted result will be stored

## Dependencies
- Functions called/Symbols referenced:
  - [PGTYPESnumeric_new](../P/PGTYPESnumeric_new.md)
  - [PGTYPESnumeric_from_decimal](../P/PGTYPESnumeric_from_decimal.md)  
  - [PGTYPESnumeric_to_long](../P/PGTYPESnumeric_to_long.md)
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
- Located in src/interfaces/ecpg/compatlib/informix.c:480-507
- Uses errno to detect overflow conditions in the underlying numeric conversion
- Companion function to `dectoint` but for long integer conversions

## Simplified Source

```c
int dectolong(decimal *np, long *lngp) {
    // Create new numeric value for intermediate conversion
    numeric *nres = PGTYPESnumeric_new();
    if (nres == NULL)
        return ECPG_INFORMIX_OUT_OF_MEMORY;

    // Convert decimal to numeric format
    if (PGTYPESnumeric_from_decimal(np, nres) != 0) {
        PGTYPESnumeric_free(nres);
        return ECPG_INFORMIX_OUT_OF_MEMORY;
    }

    // Convert numeric to long integer with overflow detection
    errno = 0;
    int ret = PGTYPESnumeric_to_long(nres, lngp);
    int errnum = errno;
    PGTYPESnumeric_free(nres);

    // Handle overflow error
    if (ret == -1 && errnum == PGTYPES_NUM_OVERFLOW)
        ret = ECPG_INFORMIX_NUM_OVERFLOW;

    return ret;
}
```