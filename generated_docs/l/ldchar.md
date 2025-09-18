# ldchar

## Location
src/interfaces/ecpg/compatlib/informix.c: 977 - 985

## Overview
The ldchar function copies a fixed-length string to a destination buffer, automatically trimming trailing spaces and null-terminating the result.

## Definition


## Detailed Description
The ldchar function is part of PostgreSQL's ECPG Informix compatibility library. It provides a convenient way to convert fixed-length character arrays (such as those used in database CHAR columns) into null-terminated C strings. The function first determines the effective length of the source string by calling byleng() to skip trailing spaces, then copies only the meaningful content to the destination and adds a null terminator.

This function is essential for working with fixed-length database fields in Informix-compatible applications, where trailing spaces are typically padding that should be removed when converting to standard C strings.

## Parameters / Member Variables
- : Source character array/string to copy from
- : Maximum length of the source string to consider
- : Destination buffer where the trimmed, null-terminated string will be stored

## Dependencies
- Functions called/Symbols referenced:
  - byleng() - to calculate effective length without trailing spaces
  - memmove() - standard C library function for memory copying
- Called from (representative examples):
  - Test cases in compat_informix-charfuncs.c
  - ECPG_INFORMIX_EXTRA_CHARS macro context

## Notes and Other Information
- The destination buffer must be large enough to hold the trimmed string plus the null terminator
- Uses memmove() instead of strcpy() to handle potential memory overlap safely
- Does not validate buffer sizes - caller must ensure destination is adequately sized
- Part of the ECPG Informix compatibility layer for handling fixed-length character data
- Located in src/interfaces/ecpg/compatlib/informix.c:977-985