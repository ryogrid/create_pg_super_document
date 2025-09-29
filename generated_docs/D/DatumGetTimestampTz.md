# DatumGetTimestampTz

## Location
[src/include/utils/timestamp.h:34-39](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/timestamp.h#L34-L39)

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
  - [DatumGetInt64](DatumGetInt64.md)
  - TimestampTz (type cast)
- Called from (representative examples):
  - [validateRecoveryParameters](../v/validateRecoveryParameters.md)
  - [get_role_password](../g/get_role_password.md)
  - [JsonEncodeDateTime](../J/JsonEncodeDateTime.md)
  - [executeDateTimeMethod](../e/executeDateTimeMethod.md)
  - [compareDatetime](../c/compareDatetime.md)
  - [convert_timevalue_to_scalar](../c/convert_timevalue_to_scalar.md)
  - PG_GETARG_TIMESTAMPTZ (macro)

## Notes and Other Information
- This function is defined as static inline for performance efficiency
- The function relies on the fact that TimestampTz and int64 have identical memory layouts
- Used in various contexts including recovery validation, authentication, JSON processing, and statistical functions
- TimestampTz differs from Timestamp conceptually (includes timezone awareness) but has the same internal representation
- Location: src/include/utils/timestamp.h:34-39

## Simplified Source

```c
static inline TimestampTz DatumGetTimestampTz(Datum X)
{
    return (TimestampTz) DatumGetInt64(X);
}
```