# anytimestamp_typmod_check

## Location
src/backend/utils/adt/timestamp.c: 123 - 144

## Overview
A validation function that checks and normalizes precision values for TIMESTAMP and TIMESTAMP WITH TIME ZONE data types, ensuring they fall within acceptable bounds.

## Definition


## Detailed Description
This function validates the precision (typmod) parameter for timestamp types, ensuring it meets PostgreSQL's constraints for timestamp precision. It performs range checking and automatic adjustment when values exceed the maximum allowed precision. The function is exported specifically so that parse_expr.c can use it during SQL parsing operations.

The function enforces that precision values are non-negative and do not exceed MAX_TIMESTAMP_PRECISION. When the precision exceeds the maximum, it issues a warning and automatically reduces the value to the maximum allowed precision rather than throwing an error.

## Parameters / Member Variables
- `istz`: Boolean flag indicating whether this is for a timezone-aware timestamp type (affects error/warning message formatting)
- `typmod`: The precision value to validate (number of fractional seconds digits, 0-6)

## Dependencies
- Functions called/Symbols referenced:
  - MAX_TIMESTAMP_PRECISION (constant defining maximum allowed precision)
  - ereport (for error and warning reporting)
- Called from:
  - anytimestamp_typmodin (src/backend/utils/adt/timestamp.c:118)
  - transformSQLValueFunction (src/backend/parser/parse_expr.c:2325, 2339)
  - executeDateTimeMethod (src/backend/utils/adt/jsonpath_exec.c:2671, 2752)
  - TimestampTzPlusSeconds (src/include/utils/timestamp.h:98)

## Notes and Other Information
- Negative precision values result in an error with appropriate error code ERRCODE_INVALID_PARAMETER_VALUE
- Precision values exceeding MAX_TIMESTAMP_PRECISION generate a warning but are automatically adjusted rather than causing an error
- The function is used both during type declaration parsing and during runtime operations involving timestamp precision
- Error and warning messages are context-aware, including "WITH TIME ZONE" in messages when istz is true
- Returns the validated (and possibly adjusted) precision value as an int32