# pg_wchar_strlen

## Location
src/backend/utils/mb/wstrncmp.c: 70 - 77

## Overview
Calculates the length of a wide character string by counting characters until a null terminator is encountered.

## Definition


## Detailed Description
This function computes the length of a wide character string (pg_wchar*) by iterating through the characters until it encounters a null terminator (0). It follows the standard strlen semantics but operates on PostgreSQL's wide character type rather than regular char arrays. The function uses pointer arithmetic to efficiently calculate the length by subtracting the original pointer position from the final position after the loop.

The implementation is optimized for simplicity and efficiency, using a straightforward for-loop that increments a pointer until it finds the terminating null character.

## Parameters / Member Variables
- : Pointer to the null-terminated wide character string whose length is to be calculated

## Dependencies
- Functions called/Symbols referenced: None (implements basic length calculation)
- Called from:
  - pg_wchar2mb (src/backend/utils/mb/mbutils.c:1003)

## Notes and Other Information
- This is a utility function for wide character string operations in PostgreSQL's multibyte character support system
- The function is implemented in src/backend/utils/mb/wstrncmp.c as part of PostgreSQL's multibyte/wide character handling infrastructure
- Follows standard C library strlen semantics but adapted for pg_wchar type
- Used by multibyte conversion functions that need to determine the length of wide character strings before processing
- The implementation assumes the input string is properly null-terminated