# garbage_left

## Location
[src/interfaces/ecpg/ecpglib/data.c:46-75](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/data.c#L46-L75)

## Overview
A validation function that detects unwanted trailing characters ("garbage") after parsing data values in PostgreSQL's ECPG (Embedded SQL in C) interface, with special handling for INFORMIX compatibility mode.

## Definition
```c
static bool garbage_left(enum ARRAY_TYPE isarray, char **scan_length, enum COMPAT_MODE compat)
```

## Detailed Description
The `garbage_left` function examines the remaining characters after data parsing to determine if there are invalid trailing characters that should not be present. This function is crucial for data validation in ECPG's type conversion and parsing operations.

The function handles different scenarios based on the array type:
- **Non-array data** (`ECPG_ARRAY_NONE`): Checks for trailing characters after the parsed value, with special INFORMIX compatibility handling for decimal numbers
- **Array data**: Uses `array_delimiter` and `array_boundary` functions to determine if trailing characters are valid array syntax

Special INFORMIX compatibility features:
- When in INFORMIX mode and processing numeric data, decimal points followed by digits are skipped (truncated) rather than treated as garbage
- This allows INFORMIX-style numeric truncation behavior

## Parameters / Member Variables
- `isarray`: An enum of type `ARRAY_TYPE` indicating whether the data is part of an array and what type
- `scan_length`: A pointer to a character pointer that points to the current position in the string being parsed
- `compat`: An enum of type `COMPAT_MODE` specifying compatibility mode (e.g., INFORMIX_MODE)

## Dependencies
- Functions called/Symbols referenced:
  - ARRAY_TYPE (enum)
  - COMPAT_MODE (enum)
  - [ECPG_ARRAY_NONE](../E/ECPG_ARRAY_NONE.md) (enum constant)
  - INFORMIX_MODE (macro/function)
  - [array_delimiter](../a/array_delimiter.md) (function)
  - [array_boundary](../a/array_boundary.md) (function)
  - ECPG_IS_ARRAY (macro)
  - isdigit (standard library function)
- Called from (representative examples):
  - [ecpg_get_data](../e/ecpg_get_data.md) (multiple locations)

## Notes and Other Information
- This is a static function, only accessible within data.c
- The function modifies the scan_length pointer when skipping invalid characters in INFORMIX mode
- Returns `true` if garbage characters are found, `false` if the remaining string is valid
- Critical for ensuring data integrity and proper error reporting in ECPG applications
- The INFORMIX compatibility feature allows for legacy application behavior where numeric truncation is expected rather than treated as an error