# check_special_value

## Location
src/interfaces/ecpg/ecpglib/data.c: 101 - 127

## Overview
Checks if a string represents a special floating-point value (NaN, Infinity, or -Infinity) and converts it to the corresponding double value.

## Definition
```c
static bool check_special_value(char *ptr, double *retval, char **endptr)
```

## Detailed Description
This static function is part of PostgreSQL's ECPG (Embedded SQL in C) library, specifically used for parsing special floating-point values from string input. It performs case-insensitive comparison to detect three special values:

1. "NaN" - Not a Number
2. "Infinity" - Positive infinity  
3. "-Infinity" - Negative infinity

When a match is found, the function sets the appropriate IEEE 754 floating-point value in the output parameter and advances the end pointer to indicate how much of the input string was consumed. This follows the standard pattern used by string-to-number conversion functions like `strtod()`.

## Parameters / Member Variables
- `ptr`: Input string pointer to check for special floating-point values
- `retval`: Output parameter where the converted double value is stored if a special value is found
- `endptr`: Output parameter set to point to the character after the parsed special value

## Dependencies
- Functions called/Symbols referenced:
  - pg_strncasecmp (case-insensitive string comparison)
  - get_float8_nan (returns IEEE 754 NaN value)
  - get_float8_infinity (returns IEEE 754 positive infinity value)
- Called from (representative examples):
  - ecpg_get_data

## Notes and Other Information
- This is a static helper function used internally within the ECPG data conversion routines
- The function follows the standard C library convention of setting an end pointer to indicate parsing progress
- Uses PostgreSQL's portable string comparison functions rather than standard C library functions for consistency
- Part of the broader ECPG infrastructure that allows embedding SQL statements in C programs