# i4tochar

## Location
src/backend/utils/adt/char.c: 190 - 203

## Overview
Converts a 32-bit signed integer to PostgreSQL's "char" (single byte character) data type with range validation.

## Definition
```c
Datum i4tochar(PG_FUNCTION_ARGS)
```

## Detailed Description
This function performs a safe conversion from a 32-bit signed integer to PostgreSQL's "char" data type. It includes bounds checking to ensure the input value falls within the valid range for a signed 8-bit character (SCHAR_MIN to SCHAR_MAX). If the input value is outside this range, the function raises an error with the code ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE and the message "char" out of range. When the value is within range, it casts the integer to an int8 and returns it as a char.

## Parameters / Member Variables
- Uses PG_FUNCTION_ARGS macro to access function arguments
- `arg1`: The input 32-bit signed integer retrieved using PG_GETARG_INT32(0)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT32 (macro for extracting int32 argument)
  - SCHAR_MIN, SCHAR_MAX (system constants for signed char range)
  - ereport (PostgreSQL error reporting function)
  - [errcode](../e/errcode.md) (PostgreSQL error code function)
  - [errmsg](../e/errmsg.md) (PostgreSQL error message function)
  - int8 (8-bit signed integer type)
  - PG_RETURN_CHAR (macro for returning char result)

- Called from (representative examples):
  - No direct references found in the codebase (likely called via SQL CAST operations)

## Notes and Other Information
- Provides safe conversion with range validation to prevent data loss
- Follows PostgreSQL's error handling conventions using ereport()
- The valid range is typically -128 to 127 for signed 8-bit characters
- Used internally by PostgreSQL's type conversion system for integer to char casts
- The function follows PostgreSQL's V1 calling convention using the PG_FUNCTION_ARGS interface