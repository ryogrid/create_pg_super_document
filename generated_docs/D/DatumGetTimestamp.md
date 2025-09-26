# DatumGetTimestamp

## Location
src/include/utils/timestamp.h: 28 - 33

## Overview
Converts a PostgreSQL Datum to a Timestamp value by leveraging int64 conversion routines, since Timestamp is internally represented as an int64 value.

## Definition


## Detailed Description
DatumGetTimestamp is an inline function that extracts a Timestamp value from a Datum. Since PostgreSQL's Timestamp type is built on top of int64 representation (microseconds since PostgreSQL epoch), this function simply delegates to DatumGetInt64 and casts the result to Timestamp. The function follows the same pass-by-reference semantics as int64, meaning Timestamp values are passed by reference if and only if int64 values are passed by reference on the target platform.

## Parameters / Member Variables
- : The input Datum containing the timestamp value to be extracted

## Dependencies
- Functions called/Symbols referenced:
  - DatumGetInt64
  - Timestamp (type cast)
- Called from (representative examples):
  - JsonEncodeDateTime
  - executeDateTimeMethod
  - compareDatetime
  - convert_timevalue_to_scalar
  - timestamp_fastcmp
  - in_range_timestamp_interval
  - generate_series_timestamp
  - map_sql_value_to_xml_value
  - PG_GETARG_TIMESTAMP (macro)

## Notes and Other Information
- This function is defined as static inline for performance efficiency
- The function relies on the fact that Timestamp and int64 have identical memory layouts
- Used extensively throughout the codebase for timestamp operations in JSON processing, statistical functions, range operations, and XML conversion
- Location: src/include/utils/timestamp.h:28-33