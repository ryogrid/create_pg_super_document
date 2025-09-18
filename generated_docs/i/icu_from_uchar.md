# icu_from_uchar

## Location
src/backend/utils/adt/pg_locale.c: 2785 - 2825

## Overview
Converts a UChar string from ICU's Unicode representation back to the database encoding, handling memory allocation and returning both the converted string and its length.

## Definition
int32_t icu_from_uchar(char **result, const UChar *buff_uchar, int32_t len_uchar)

## Detailed Description
This function performs the reverse conversion of icu_to_uchar, converting Unicode strings (UChar arrays) back to PostgreSQL's database encoding. It implements a two-phase approach typical of ICU conversion functions:

1. First phase: Calls ucnv_fromUChars() with a NULL destination to determine the required buffer size
2. Second phase: Allocates the appropriate buffer and performs the actual conversion

The function ensures proper error handling throughout both phases and handles special ICU status codes like U_BUFFER_OVERFLOW_ERROR (expected in phase 1) and U_STRING_NOT_TERMINATED_WARNING (treated as an error). This is the complementary operation to icu_to_uchar and is essential for returning processed Unicode strings back to PostgreSQL's native encoding.

## Parameters / Member Variables
- : Output parameter that receives a pointer to the allocated result string in database encoding
- : Source UChar string in ICU Unicode format to convert
- : Length of the source UChar string (need not be null-terminated)

## Dependencies
- Functions called/Symbols referenced:
  - [init_icu_converter](init_icu_converter.md) (ensures ICU converter is ready)
  - ucnv_fromUChars (ICU function to convert from Unicode - called twice)
  - u_errorName (ICU function to get error name string)
  - [palloc](../p/palloc.md) (PostgreSQL memory allocation)
- Called from (representative examples):
  - [str_tolower](../s/str_tolower.md) (string case conversion functions)
  - [str_toupper](../s/str_toupper.md) (string case conversion functions)
  - [str_initcap](../s/str_initcap.md) (string capitalization functions)
  - pg_locale_t (locale-related operations)

## Notes and Other Information
- This is a public function accessible outside pg_locale.c (not static)
- Returns int32_t representing the length in bytes of the converted result (excluding null terminator)
- The output string is null-terminated for compatibility with C string functions
- Memory allocation uses palloc(), so the result is automatically freed when the memory context is reset
- Uses a two-phase conversion approach for proper buffer sizing and memory safety
- Treats U_STRING_NOT_TERMINATED_WARNING as an error, ensuring proper string termination
- Essential counterpart to icu_to_uchar for round-trip Unicode processing
- Used primarily in string processing functions that need to return results in database encoding
- The function handles the complexity of ICU's conversion API, providing a simple interface for PostgreSQL code