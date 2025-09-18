# pg_get_utf8_id

## Location
src/fe_utils/mbprint.c: 34 - 42

## Overview
A static utility function that returns the character encoding ID for UTF-8, caching the result to avoid repeated lookups.

## Definition


## Detailed Description
This function serves as a cached accessor for the UTF-8 character encoding ID in PostgreSQL's frontend utilities. It uses a static variable to store the encoding ID after the first lookup, implementing a simple memoization pattern. The function calls  only once and stores the result for subsequent calls, improving performance when UTF-8 encoding ID is needed multiple times.

## Parameters / Member Variables
This function takes no parameters and returns an integer representing the UTF-8 encoding ID.

## Dependencies
- Functions called/Symbols referenced:
  - pg_char_to_encoding
- Called from (representative examples):
  - PG_UTF8 (macro)

## Notes and Other Information
- The function is declared as static, limiting its scope to the mbprint.c file
- Uses lazy initialization pattern - the encoding ID is only looked up when first needed
- The static variable  is initialized to -1 to indicate it hasn't been set yet
- This is part of PostgreSQL's frontend utilities for handling multibyte character printing