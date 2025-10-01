# DatumGetTimestamp

## Location
[src/include/utils/timestamp.h:28-33](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/timestamp.h#L28-L33)

## Overview
Converts a PostgreSQL Datum to a Timestamp value by leveraging int64 conversion routines, since Timestamp is internally represented as an int64 value.

## Definition

```c
static inline Timestamp
DatumGetTimestamp(Datum X)
```
## Detailed Description
DatumGetTimestamp is an inline function that extracts a Timestamp value from a Datum. Since PostgreSQL's Timestamp type is built on top of int64 representation (microseconds since PostgreSQL epoch), this function simply delegates to DatumGetInt64 and casts the result to Timestamp. The function follows the same pass-by-reference semantics as int64, meaning Timestamp values are passed by reference if and only if int64 values are passed by reference on the target platform.

## Parameters / Member Variables
- : The input Datum containing the timestamp value to be extracted

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetInt64](DatumGetInt64.md)
  - Timestamp (type cast)
- Called from (representative examples):
  - [JsonEncodeDateTime](../J/JsonEncodeDateTime.md)
  - [executeDateTimeMethod](../e/executeDateTimeMethod.md)
  - [compareDatetime](../c/compareDatetime.md)
  - [convert_timevalue_to_scalar](../c/convert_timevalue_to_scalar.md)
  - [timestamp_fastcmp](../t/timestamp_fastcmp.md)
  - [in_range_timestamp_interval](../i/in_range_timestamp_interval.md)
  - [generate_series_timestamp](../g/generate_series_timestamp.md)
  - [map_sql_value_to_xml_value](../m/map_sql_value_to_xml_value.md)
  - PG_GETARG_TIMESTAMP (macro)

## Notes and Other Information
- This function is defined as static inline for performance efficiency
- The function relies on the fact that Timestamp and int64 have identical memory layouts
- Used extensively throughout the codebase for timestamp operations in JSON processing, statistical functions, range operations, and XML conversion
- Location: src/include/utils/timestamp.h:28-33

## Simplified Source

```c
static inline Timestamp
DatumGetTimestamp(Datum X)
{
    // Convert Datum to Timestamp by extracting as 64-bit integer
    // Timestamp is internally represented as int64 (microseconds since PostgreSQL epoch)
    return (Timestamp) DatumGetInt64(X);
}
```