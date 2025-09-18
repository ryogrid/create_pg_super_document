# deccvdbl

## Location
src/interfaces/ecpg/compatlib/informix.c: 246 - 267

## Overview
Converts a double-precision floating-point value to a decimal data type, providing Informix-compatible decimal conversion functionality in PostgreSQL ECPG.

## Definition


## Detailed Description
The `deccvdbl` function is part of the PostgreSQL ECPG Informix compatibility library that converts a double-precision floating-point number to a decimal data type. It performs null input validation, creates a new numeric value using PostgreSQL's numeric functions, converts the double to numeric format, and then converts the numeric to decimal format. The function includes comprehensive error handling for memory allocation failures and conversion errors.

This function maintains Informix semantics for decimal conversion, ensuring compatibility when porting Informix applications to PostgreSQL ECPG.

## Parameters / Member Variables
- `dbl`: Source double-precision floating-point value to convert (double)
- `np`: Pointer to the decimal structure to store the converted value (decimal *)

## Dependencies
- Functions called/Symbols referenced:
  - [rsetnull](../r/rsetnull.md)
  - [risnull](../r/risnull.md)
  - [PGTYPESnumeric_new](../P/PGTYPESnumeric_new.md)
  - [PGTYPESnumeric_from_double](../P/PGTYPESnumeric_from_double.md)
  - PGTYPESnumeric_to_decimal
  - [PGTYPESnumeric_free](../P/PGTYPESnumeric_free.md)
- Called from (representative examples):
  - ECPG_INFORMIX_EXTRA_CHARS

## Notes and Other Information
- Returns 0 on success, or specific error codes for different failure scenarios
- Returns ECPG_INFORMIX_OUT_OF_MEMORY if numeric allocation fails
- Properly handles null inputs using rsetnull and risnull functions with CDOUBLETYPE and CDECIMALTYPE
- Uses PostgreSQL's numeric type as an intermediate format for precision preservation
- Includes proper memory management with PGTYPESnumeric_free cleanup
- Part of the Informix compatibility layer in src/interfaces/ecpg/compatlib/informix.c
- Maintains precision during conversion from double to decimal format