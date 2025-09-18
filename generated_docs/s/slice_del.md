# slice_del

## Location
[src/backend/snowball/libstemmer/utilities.c:431-434](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/utilities.c#L431-L434)

## Overview
A convenience function in the Snowball stemming library that deletes the currently selected slice from the working string by replacing it with an empty string.

## Definition


## Detailed Description
The  function is a simple wrapper around  that effectively deletes the current slice (the substring between  and  positions) from the working string. It works by replacing the selected slice with an empty string (0 characters, NULL pointer). This is a common operation in stemming algorithms where portions of words need to be removed.

The function operates on the Snowball environment structure which maintains the current string being processed and the boundary markers ( and ) that define the slice to be operated on.

## Parameters / Member Variables
- : Pointer to the Snowball environment structure () containing:
  - : The working string buffer
  - : Start position of the current slice
  - : End position of the current slice
  - Other state variables for string manipulation

## Dependencies
- Functions called/Symbols referenced:
  - [slice_from_s](slice_from_s.md) (called with empty string parameters to achieve deletion)
- Called from (representative examples):
  - Various stemming algorithm functions in generated stemmer code

## Notes and Other Information
- This function is part of the Snowball stemming library utilities
- Returns the same value as  which is typically 0 on success or -1 on error
- The actual deletion is performed by the underlying  and  functions
- Used extensively in generated stemming code for different languages
- Part of the external API for Snowball stemmer implementations