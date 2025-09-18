# deccvlong

## Location
src/interfaces/ecpg/compatlib/informix.c: 290 - 311

## Overview
Converts a long integer value to a decimal data type, providing Informix-compatible decimal conversion functionality in PostgreSQL ECPG.

## Definition


## Detailed Description
The `deccvlong` function is part of the PostgreSQL ECPG Informix compatibility library that converts a long integer value to a decimal data type. It performs null input validation using the CLONGTYPE, creates a new numeric value using PostgreSQL's numeric functions, converts the long integer to numeric format, and then converts the numeric to decimal format. The function includes comprehensive error handling for memory allocation failures and conversion errors.

This function maintains Informix semantics for decimal conversion, ensuring seamless compatibility when porting Informix applications that use long integer types to PostgreSQL ECPG.

## Parameters / Member Variables
- `lng`: Source long integer value to convert (long)
- `np`: Pointer to the decimal structure to store the converted value (decimal *)

## Dependencies
- Functions called/Symbols referenced:
  - rsetnull
  - risnull
  - PGTYPESnumeric_new
  - PGTYPESnumeric_from_long
  - PGTYPESnumeric_to_decimal
  - PGTYPESnumeric_free
- Called from (representative examples):
  - main (in test cases)
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