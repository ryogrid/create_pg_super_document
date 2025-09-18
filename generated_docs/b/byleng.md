# byleng

## Location
src/interfaces/ecpg/compatlib/informix.c: 970 - 976

## Overview
The byleng function calculates the effective length of a string by trimming trailing spaces from a fixed-length string.

## Definition


## Detailed Description
The byleng function is part of PostgreSQL's ECPG Informix compatibility library. It determines the actual length of a string by removing trailing spaces from a fixed-length character array. The function starts from the end of the specified length and works backwards, skipping over space characters until it finds a non-space character or reaches the beginning of the string.

This function is commonly used with fixed-length character fields (like CHAR columns in databases) where trailing spaces are often padded but not considered part of the meaningful content.

## Parameters / Member Variables
- : A character string/array to measure
- : The maximum length to consider (typically the allocated size of the string)

## Dependencies
- Functions called/Symbols referenced:
  - None (uses only basic C operations)
- Called from (representative examples):
  - [ldchar](../l/ldchar.md)() function in the same file
  - Test cases in compat_informix-charfuncs.c
  - ECPG_INFORMIX_EXTRA_CHARS macro context

## Notes and Other Information
- Returns the effective length of the string (1-based, not 0-based)
- Does not modify the input string, only calculates length
- Specifically designed for Informix compatibility where fixed-length strings are common
- Handles the case where the entire string might be spaces
- Located in src/interfaces/ecpg/compatlib/informix.c:970-976