# newabbr

## Location
src/timezone/zic.c: 3910 - 3947

## Overview
Validates and registers a new timezone abbreviation string, ensuring it meets POSIX standards and length requirements.

## Definition
```c
static void newabbr(const char *string)
```

## Detailed Description
The `newabbr` function processes and validates timezone abbreviation strings before adding them to the global character array. It performs comprehensive validation including:

1. Checks if the abbreviation matches the special GRANDPARENTED constant (which bypasses validation)
2. Validates character content - allows only alphanumeric characters, hyphens, and plus signs
3. Enforces minimum length requirement (3 characters) when noise warnings are enabled
4. Checks maximum length against ZIC_MAX_ABBR_LEN_WO_WARN threshold
5. Ensures string contains only valid POSIX characters
6. Verifies total character storage limit is not exceeded
7. Copies the validated string to the global chars array and updates the character count

The function maintains a global character pool for all timezone abbreviations used in the compiled timezone data.

## Parameters / Member Variables
- `string`: The timezone abbreviation string to validate and add

## Dependencies
- Functions called/Symbols referenced:
  - `GRANDPARENTED`: Special constant for legacy timezone abbreviations
  - `is_alpha`: Character classification function for alphabetic characters
  - [warning](../w/warning.md): Warning message function for validation issues
  - [error](../e/error.md): Error reporting function for fatal conditions
  - `strlen`: Standard C string length function
  - `strcpy`: Standard C string copy function
- Called from (representative examples):
  - [addtype](../a/addtype.md): Function that adds new timezone types

## Notes and Other Information
- Uses global variables `chars` (character array) and `charcnt` (character count) for storage
- GRANDPARENTED abbreviations bypass all validation checks
- Exits with EXIT_FAILURE if total character limit (TZ_MAX_CHARS) is exceeded
- Warnings are only issued when the global `noise` flag is enabled
- Enforces POSIX compliance for timezone abbreviation format
- Part of the zic (zone information compiler) timezone data generation system