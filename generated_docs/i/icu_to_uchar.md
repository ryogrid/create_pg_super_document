# icu_to_uchar

## Location
src/backend/utils/adt/pg_locale.c: 2758 - 2784

## Overview
Converts a string from the database encoding to ICU's Unicode (UChar) representation, handling memory allocation and returning both the converted string and its length.

## Definition
int32_t icu_to_uchar(UChar **buff_uchar, const char *buff, size_t nbytes)

## Detailed Description
This is a high-level convenience function that orchestrates the complete conversion process from PostgreSQL's database encoding to ICU Unicode format. It combines the functionality of several lower-level functions to provide a simple interface for string conversion:

1. Ensures the ICU converter is initialized
2. Calculates the required buffer size for the converted string
3. Allocates memory for the result (including null termination)
4. Performs the actual conversion
5. Returns both the converted string (via output parameter) and its length

The function handles all the complexity of ICU string conversion, including proper memory allocation and error handling, making it the primary interface for converting PostgreSQL strings to ICU format.

## Parameters / Member Variables
- : Output parameter that receives a pointer to the allocated UChar string result
- : Source string in database encoding to convert
- : Length of the source string in bytes (need not be null-terminated)

## Dependencies
- Functions called/Symbols referenced:
  - init_icu_converter (ensures ICU converter is ready)
  - uchar_length (calculates required buffer size)
  - uchar_convert (performs the actual conversion)
  - palloc (PostgreSQL memory allocation)
- Called from (representative examples):
  - str_tolower (string case conversion functions)
  - str_toupper (string case conversion functions) 
  - str_initcap (string capitalization functions)
  - make_icu_collator (collation setup)
  - pg_locale_t (locale-related operations)

## Notes and Other Information
- This is a public function accessible outside pg_locale.c (not static)
- Returns int32_t representing the length of the converted UChar string
- The output string is null-terminated for compatibility, though length is also provided
- Memory allocation uses palloc(), so the result is automatically freed when the memory context is reset
- The caller is responsible for the lifetime management of the returned buffer
- This function is the primary entry point for database-to-ICU string conversion
- Essential for ICU-based text processing operations like collation, case conversion, and normalization
- The function gracefully handles strings that don't need to be null-terminated in the source