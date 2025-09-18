# asc_tolower_z

## Location
src/backend/utils/adt/formatting.c: 2253 - 2258

## Overview
A convenience wrapper function that converts a null-terminated string to lowercase using ASCII-only character transformation.

## Definition


## Detailed Description
This function is a simplified wrapper around the  function that automatically determines the string length using . It provides ASCII-only lowercase conversion for null-terminated strings, eliminating the need for the caller to specify the buffer length explicitly. The function is static to the formatting.c module and is primarily used within PostgreSQL's numeric formatting operations.

## Parameters / Member Variables
- : A null-terminated input string to be converted to lowercase

## Dependencies
- Functions called/Symbols referenced:
  - asc_tolower
  - strlen
- Called from (representative examples):
  - NUM_processor

## Notes and Other Information
- This is a static function local to src/backend/utils/adt/formatting.c
- The function assumes the input string is null-terminated, unlike its parent function  which accepts a byte count
- Returns a palloc'd string that must be freed by the caller
- Used specifically in numeric formatting operations within PostgreSQL's formatting system
- The 'z' suffix indicates this variant works with null-terminated (zero-terminated) strings