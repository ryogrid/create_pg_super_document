# icu_convert_case

## Location
[src/backend/utils/adt/formatting.c:1581-1607](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/formatting.c#L1581-L1607)

## Overview
A utility function that performs locale-aware case conversion using ICU (International Components for Unicode) library functions with proper buffer management and error handling.

## Definition


## Detailed Description
The  function serves as a generic wrapper for ICU case conversion operations (uppercase, lowercase, title case). It handles the complexity of ICU's buffer management by initially attempting conversion with a buffer of the same size as the source, then reallocating with the correct size if a buffer overflow occurs. This two-pass approach is necessary because ICU case conversion can result in strings of different lengths than the original (due to locale-specific rules, ligatures, or special character mappings).

The function provides robust error handling and memory management, ensuring that case conversion operations work correctly across all Unicode characters and locale-specific rules. It abstracts away the ICU-specific details and provides a consistent interface for PostgreSQL's string formatting functions.

## Parameters / Member Variables
- : Function pointer to the specific ICU case conversion function (e.g., u_strToUpper, u_strToLower)
- : PostgreSQL locale structure containing ICU locale information
- : Pointer to destination buffer pointer (allocated by this function)
- : Source Unicode string buffer to be converted
- : Length of the source buffer in UChar units

## Dependencies
- Functions called/Symbols referenced:
  - pg_locale_t (type)
  - [palloc](../p/palloc.md)
  - [pfree](../p/pfree.md)
  - ereport
  - ERROR
  - [errmsg](../e/errmsg.md)
  - u_errorName
  - UErrorCode
  - UChar
  - ICU_Convert_Func
  - U_ZERO_ERROR
  - U_BUFFER_OVERFLOW_ERROR
  - U_FAILURE
- Called from (representative examples):
  - [str_tolower](../s/str_tolower.md)
  - [str_toupper](../s/str_toupper.md)  
  - [str_initcap](../s/str_initcap.md)

## Notes and Other Information
- This is a static function only available within the formatting.c compilation unit
- The function implements a two-pass buffer allocation strategy to handle cases where the converted string is longer than the source
- Caller is responsible for freeing the allocated destination buffer after use
- The function supports all ICU case conversion functions through the function pointer parameter
- Proper error reporting converts ICU error codes to PostgreSQL error messages
- Used extensively in PostgreSQL's locale-aware string functions that need to handle international character sets correctly
- The function handles buffer overflow gracefully by reallocating with the correct size as determined by ICU
- Returns the actual length of the converted string in UChar units