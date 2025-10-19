# deccvlong

## Location
[src/interfaces/ecpg/compatlib/informix.c:290-311](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/compatlib/informix.c#L290-L311)

## Overview
Converts a long integer value to a decimal data type, providing Informix-compatible decimal conversion functionality in PostgreSQL ECPG.

## Definition

```c
int
deccvlong(long lng, decimal *np)
```
## Detailed Description
The `deccvlong` function is part of the PostgreSQL ECPG Informix compatibility library that converts a long integer value to a decimal data type. It performs null input validation using the CLONGTYPE, creates a new numeric value using PostgreSQL's numeric functions, converts the long integer to numeric format, and then converts the numeric to decimal format. The function includes comprehensive error handling for memory allocation failures and conversion errors.

This function maintains Informix semantics for decimal conversion, ensuring seamless compatibility when porting Informix applications that use long integer types to PostgreSQL ECPG.

## Parameters / Member Variables
- `lng`: Source long integer value to convert (long)
- `np`: Pointer to the decimal structure to store the converted value (decimal *)

## Dependencies
- Functions called/Symbols referenced:
  - [rsetnull](../r/rsetnull.md)
  - [risnull](../r/risnull.md)
  - [PGTYPESnumeric_new](../P/PGTYPESnumeric_new.md)
  - [PGTYPESnumeric_from_long](../P/PGTYPESnumeric_from_long.md)
  - [PGTYPESnumeric_to_decimal](../P/PGTYPESnumeric_to_decimal.md)
  - [PGTYPESnumeric_free](../P/PGTYPESnumeric_free.md)
- Called from (representative examples):
  - [main](../m/main.md) (in test cases)
  - ECPG_INFORMIX_EXTRA_CHARS

## Notes and Other Information
- Returns 0 on success, or specific error codes for different failure scenarios
- Returns ECPG_INFORMIX_OUT_OF_MEMORY if numeric allocation fails
- Properly handles null inputs using rsetnull and risnull functions with CLONGTYPE and CDECIMALTYPE
- Uses PostgreSQL's numeric type as an intermediate format for precision preservation
- Includes proper memory management with PGTYPESnumeric_free cleanup
- Part of the Informix compatibility layer in src/interfaces/ecpg/compatlib/informix.c
- Handles larger integer values than deccvint due to long integer range
- Maintains exact precision when converting from long integer to decimal format
- Used in test cases for validating long integer to decimal conversions

## Simplified Source

```c
int deccvlong(long lng, decimal *np) {
    // Initialize output decimal as null
    rsetnull(CDECIMALTYPE, (char *) np);

    // Handle null input
    if (risnull(CLONGTYPE, (char *) &lng))
        return 0;

    // Create new numeric value
    numeric *nres = PGTYPESnumeric_new();
    if (nres == NULL)
        return ECPG_INFORMIX_OUT_OF_MEMORY;

    // Convert long integer to numeric, then numeric to decimal
    int result = PGTYPESnumeric_from_long(lng, nres);
    if (result == 0)
        result = PGTYPESnumeric_to_decimal(nres, np);

    PGTYPESnumeric_free(nres);
    return result;
}
```