# asc_toupper_z

## Location
[src/backend/utils/adt/formatting.c:2259-2272](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/formatting.c#L2259-L2272)

## Overview
A convenience wrapper function that converts a null-terminated string to uppercase using ASCII-only character transformation.

## Definition
```c
static char *asc_toupper_z(const char *buff)
```

## Detailed Description
This function is a simplified wrapper around the `asc_toupper` function that automatically determines the string length using `strlen()`. It provides ASCII-only uppercase conversion for null-terminated strings, eliminating the need for the caller to specify the buffer length explicitly. The function is static to the formatting.c module and appears to be defined but currently unused in the PostgreSQL codebase.

## Parameters / Member Variables
- `buff`: A null-terminated input string to be converted to uppercase

## Dependencies
- Functions called/Symbols referenced:
  - [asc_toupper](asc_toupper.md)
  - strlen
- Called from (representative examples):
  - Currently no callers found in the codebase

## Notes and Other Information
- This is a static function local to src/backend/utils/adt/formatting.c
- The function assumes the input string is null-terminated, unlike its parent function `asc_toupper` which accepts a byte count
- Returns a palloc'd string that must be freed by the caller
- The 'z' suffix indicates this variant works with null-terminated (zero-terminated) strings
- Currently appears to be unused in the codebase, potentially prepared for future use or legacy code