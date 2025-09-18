# deccvasc

## Location
src/interfaces/ecpg/compatlib/informix.c: 198 - 245

## Overview
Converts a character string to a decimal value with length specification, providing Informix-compatible decimal conversion functionality in PostgreSQL ECPG.

## Definition


## Detailed Description
The `deccvasc` function is part of the PostgreSQL ECPG Informix compatibility library that converts a character string representation of a number to a decimal data type. It handles null input validation, creates a properly null-terminated string copy using `ecpg_strndup`, and performs the conversion using PostgreSQL's numeric functions. The function provides comprehensive error handling for various numeric conversion scenarios including overflow, underflow, bad numeric format, and invalid exponents.

The function follows Informix semantics for decimal conversion, making it easier to port Informix applications to PostgreSQL ECPG.

## Parameters / Member Variables
- `cp`: Source character string containing the numeric representation (const char *)
- `len`: Maximum number of characters to process from the string (int)
- `np`: Pointer to the decimal structure to store the converted value (decimal *)

## Dependencies
- Functions called/Symbols referenced:
  - [ecpg_strndup](../e/ecpg_strndup.md)
  - [rsetnull](../r/rsetnull.md)
  - [risnull](../r/risnull.md)
  - [PGTYPESnumeric_from_asc](../P/PGTYPESnumeric_from_asc.md)
  - PGTYPESnumeric_to_decimal
  - [PGTYPESnumeric_free](../P/PGTYPESnumeric_free.md)
  - free
- Called from (representative examples):
  - [main](../m/main.md) (in test cases)
  - ECPG_INFORMIX_EXTRA_CHARS

## Notes and Other Information
- Returns 0 on success, or specific error codes for different failure scenarios
- Error codes include ECPG_INFORMIX_NUM_OVERFLOW, ECPG_INFORMIX_NUM_UNDERFLOW, ECPG_INFORMIX_BAD_NUMERIC, and ECPG_INFORMIX_BAD_EXPONENT
- Properly handles null inputs using rsetnull and risnull functions
- Uses PostgreSQL's numeric type internally for precision and accuracy
- Memory management includes proper cleanup of allocated strings and numeric values
- Part of the Informix compatibility layer in src/interfaces/ecpg/compatlib/informix.c