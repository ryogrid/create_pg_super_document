# TimestampGetDatum

## Location
[src/include/utils/timestamp.h:46-51](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/timestamp.h#L46-L51)

## Overview
Converts a PostgreSQL Timestamp value to a Datum by leveraging int64 conversion routines, since Timestamp is internally represented as an int64 value.

## Definition
```c
static inline Datum TimestampGetDatum(Timestamp X)
```

## Detailed Description
TimestampGetDatum is an inline function that converts a Timestamp value into a Datum. Since PostgreSQL's Timestamp type is built on top of int64 representation (microseconds since PostgreSQL epoch), this function simply delegates to Int64GetDatum to perform the conversion. This is the inverse operation of DatumGetTimestamp and is used when returning timestamp values from PostgreSQL functions or storing them in tuple slots.

## Parameters / Member Variables
- `X`: The input Timestamp value to be converted to a Datum

## Dependencies
- Functions called/Symbols referenced:
  - Int64GetDatum
  - Timestamp (parameter type)
- Called from (representative examples):
  - ExecEvalSQLValueFunction
  - in_range_date_interval
  - date_pl_interval
  - date_mi_interval
  - parse_datetime
  - executeDateTimeMethod
  - timestamp_mi_interval
  - in_range_timestamp_interval
  - generate_series_timestamp
  - PG_RETURN_TIMESTAMP (macro)

## Notes and Other Information
- This function is defined as static inline for performance efficiency
- The function relies on the fact that Timestamp and int64 have identical memory layouts
- Used extensively throughout the codebase for returning timestamp results from functions
- Common in date/time arithmetic operations, formatting functions, and JSON processing
- Essential for the PostgreSQL function manager (fmgr) interface when returning timestamp values
- Location: src/include/utils/timestamp.h:46-51