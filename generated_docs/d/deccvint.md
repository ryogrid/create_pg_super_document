# deccvint

## Location
src/interfaces/ecpg/compatlib/informix.c: 268 - 289

## Overview
Converts an integer value to a decimal data type, providing Informix-compatible decimal conversion functionality in PostgreSQL ECPG.

## Definition


## Detailed Description
The `deccvint` function is part of the PostgreSQL ECPG Informix compatibility library that converts an integer value to a decimal data type. It performs null input validation using the CINTTYPE, creates a new numeric value using PostgreSQL's numeric functions, converts the integer to numeric format, and then converts the numeric to decimal format. The function includes comprehensive error handling for memory allocation failures and conversion errors.

This function maintains Informix semantics for decimal conversion, ensuring seamless compatibility when porting Informix applications to PostgreSQL ECPG.

## Parameters / Member Variables
- `in`: Source integer value to convert (int)
- `np`: Pointer to the decimal structure to store the converted value (decimal *)

## Dependencies
- Functions called/Symbols referenced:
  - [rsetnull](../r/rsetnull.md)
  - [risnull](../r/risnull.md)
  - [PGTYPESnumeric_new](../P/PGTYPESnumeric_new.md)
  - [PGTYPESnumeric_from_int](../P/PGTYPESnumeric_from_int.md)
  - PGTYPESnumeric_to_decimal
  - [PGTYPESnumeric_free](../P/PGTYPESnumeric_free.md)
- Called from (representative examples):
  - [main](../m/main.md) (in multiple test cases)
  - ECPG_INFORMIX_EXTRA_CHARS

## Notes and Other Information
- Returns 0 on success, or specific error codes for different failure scenarios
- Returns ECPG_INFORMIX_OUT_OF_MEMORY if numeric allocation fails
- Properly handles null inputs using rsetnull and risnull functions with CINTTYPE and CDECIMALTYPE
- Uses PostgreSQL's numeric type as an intermediate format for precision preservation
- Includes proper memory management with PGTYPESnumeric_free cleanup
- Part of the Informix compatibility layer in src/interfaces/ecpg/compatlib/informix.c
- Widely used in test cases for validating integer to decimal conversions
- Maintains exact precision when converting from integer to decimal format