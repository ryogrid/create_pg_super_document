# pg_ascii_tolower

## Location
src/port/pgstrcasecmp.c: 146 - 151

## Overview
Converts an ASCII uppercase character to its lowercase equivalent following C/POSIX locale rules, providing a locale-independent character case conversion for the ASCII character set.

## Definition


## Detailed Description
 is a utility function that performs ASCII character case conversion from uppercase to lowercase. Unlike standard library functions like , this function operates independently of the current locale, ensuring consistent behavior across different system locales. It specifically handles ASCII characters 'A' through 'Z' by adding the offset between 'A' and 'a' (32) to convert them to their lowercase equivalents 'a' through 'z'. Characters that are not uppercase ASCII letters are returned unchanged.

The function is designed to provide reliable, predictable behavior for ASCII character processing in PostgreSQL, which is crucial for database operations that need to be independent of locale settings.

## Parameters / Member Variables
- : The input character (as unsigned char) to be converted to lowercase

## Dependencies
- Functions called/Symbols referenced:
  - (None - uses only basic arithmetic operations)
- Called from (representative examples):
  - pg_wc_tolower (in regex processing)
  - asc_tolower (in formatting functions)
  - asc_initcap (in formatting functions) 
  - seq_search_ascii (in formatting functions)
  - SB_lower_char (in LIKE pattern matching)

## Notes and Other Information
- This function operates only on ASCII characters (0-127) and is locale-independent
- The conversion formula used is:  for characters in range 'A' to 'Z'
- Non-uppercase ASCII characters and all non-ASCII characters are returned unchanged
- This function is part of PostgreSQL's portable string handling utilities located in 
- It's commonly used in text processing, pattern matching, and formatting operations where case-insensitive ASCII comparisons are needed
- The function ensures consistent behavior regardless of the system's locale settings, which is important for database reliability