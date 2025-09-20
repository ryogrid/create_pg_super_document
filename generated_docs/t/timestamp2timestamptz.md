# timestamp2timestamptz

## Location
[src/backend/utils/adt/timestamp.c:6356-6364](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L6356-L6364)

## Overview
A convenience wrapper function that converts timestamp to timestamptz, throwing an error if the conversion would result in overflow.

## Definition

```c
struct pg_tm tt,
			   *tm = &tt;
```
## Detailed Description
This static function serves as a simplified interface to timestamp2timestamptz_opt_overflow, specifically for cases where overflow should result in an error rather than being handled gracefully. By passing NULL as the overflow parameter to timestamp2timestamptz_opt_overflow, this function ensures that any out-of-range conversion will throw an error with the appropriate error code (ERRCODE_DATETIME_VALUE_OUT_OF_RANGE).

The function promotes a timestamp (without timezone information) to a timestamptz (with timezone information) by interpreting the timestamp as being in the current session timezone and converting it to UTC.

## Parameters / Member Variables
-  (Timestamp): The input timestamp value to be converted to timestamptz

## Dependencies
- Functions called/Symbols referenced:
  - [timestamp2timestamptz_opt_overflow](timestamp2timestamptz_opt_overflow.md)
- Called from (representative examples):
  - [make_timestamptz](../m/make_timestamptz.md) (in src/backend/utils/adt/timestamp.c:687)
  - [timestamp_timestamptz](timestamp_timestamptz.md) (in src/backend/utils/adt/timestamp.c:6290)
  - Referenced in context of IA_TOTAL_COUNT (in src/backend/utils/adt/timestamp.c:94)

## Notes and Other Information
- This function is marked as static, indicating it's intended for internal use within the timestamp.c module
- Provides a clean, error-throwing interface for timestamp to timestamptz conversion
- Used by higher-level functions that expect conversion failures to be handled via exceptions
- The function delegates all actual conversion logic to timestamp2timestamptz_opt_overflow
- Part of PostgreSQL's layered approach to timestamp conversion, offering both error-throwing and overflow-handling variants
- Located in src/backend/utils/adt/timestamp.c:6356-6364
- Commonly used in timestamp construction and SQL function implementations where overflow should be treated as an error