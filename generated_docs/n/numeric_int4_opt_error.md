# numeric_int4_opt_error

## Location
src/backend/utils/adt/numeric.c: 4413 - 4462

## Overview
Converts a Numeric value to a 32-bit signed integer with optional error handling instead of throwing exceptions.

## Definition
```c
int32 numeric_int4_opt_error(Numeric num, bool *have_error)
```

## Detailed Description
This function provides a safe conversion from PostgreSQL's Numeric data type to a 32-bit signed integer (int32) with configurable error handling behavior. When the have_error parameter is provided (non-NULL), the function will set the error flag and return 0 instead of throwing exceptions for invalid conversions. When have_error is NULL, the function behaves like the standard conversion and throws appropriate PostgreSQL errors.

The function handles several error conditions:
- Special numeric values (NaN and infinity)
- Values that are outside the range of 32-bit signed integers
- Invalid numeric representations

This dual-mode behavior makes the function suitable for both contexts where exceptions are desired (SQL operations) and where error handling needs to be managed programmatically (internal operations, JSON processing).

## Parameters / Member Variables
- `num`: The Numeric value to be converted to int32
- `have_error`: Pointer to a boolean flag for error reporting (can be NULL)
  - If NULL: function throws errors on invalid conversions
  - If non-NULL: function sets *have_error = true and returns 0 on errors

## Dependencies
- Functions called/Symbols referenced:
  - NUMERIC_IS_SPECIAL (macro to check for NaN/infinity)
  - NUMERIC_IS_NAN (macro to check for NaN)
  - init_var_from_num (initialize NumericVar from Numeric)
  - numericvar_to_int32 (perform the actual conversion)
  - ereport (PostgreSQL error reporting)
  - errcode/errmsg (error code and message macros)
- Called from (representative examples):
  - executeItemOptUnwrapTarget (JSON path execution with error handling)
  - executeDateTimeMethod (JSON date/time processing)
  - getArrayIndex (JSON array indexing)
  - numeric_int4 (standard SQL conversion function)

## Notes and Other Information
- Located in src/backend/utils/adt/numeric.c:4413-4462
- Essential for JSON processing where type conversions may fail and need graceful error handling
- Provides range checking to ensure the numeric value fits within int32 bounds (-2,147,483,648 to 2,147,483,647)
- Used extensively in JSON path operations where numeric values might need to be converted to integers for array indexing or other operations
- The function follows PostgreSQL's pattern of offering both exception-throwing and error-flag variants for type conversions