# DatumGetTimestampTz

## Location
src/include/utils/timestamp.h: 34 - 39

## Overview
Converts a PostgreSQL Datum to a TimestampTz (timestamp with timezone) value by leveraging int64 conversion routines, since TimestampTz is internally represented as an int64 value.

## Definition
```c
static inline TimestampTz DatumGetTimestampTz(Datum X)
```

## Detailed Description
DatumGetTimestampTz is an inline function that extracts a TimestampTz value from a Datum. Like Timestamp, TimestampTz is built on top of int64 representation (microseconds since PostgreSQL epoch), so this function delegates to DatumGetInt64 and casts the result to TimestampTz. The function follows the same pass-by-reference semantics as int64, meaning TimestampTz values are passed by reference if and only if int64 values are passed by reference on the target platform.

## Parameters / Member Variables
- `X`: The input Datum containing the timestamp with timezone value to be extracted

## Dependencies
- Functions called/Symbols referenced:
  - DatumGetInt64
  - TimestampTz (type cast)
- Called from (representative examples):
  - validateRecoveryParameters
  - get_role_password
  - JsonEncodeDateTime
  - executeDateTimeMethod
  - compareDatetime
  - convert_timevalue_to_scalar
  - PG_GETARG_TIMESTAMPTZ (macro)

## Notes and Other Information
- This function is defined as static inline for performance efficiency
- The function relies on the fact that TimestampTz and int64 have identical memory layouts
- Used in various contexts including recovery validation, authentication, JSON processing, and statistical functions
- TimestampTz differs from Timestamp conceptually (includes timezone awareness) but has the same internal representation
- Location: src/include/utils/timestamp.h:34-39