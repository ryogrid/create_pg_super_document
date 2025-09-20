# timestamptz2timestamp

## Location
[src/backend/utils/adt/timestamp.c:6373-6401](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L6373-L6401)

## Overview
This static function converts a timestamp with time zone (TimestampTz) to a local timestamp without time zone by decomposing the timestamptz value and reconstructing it as a plain timestamp.

## Definition

```c
struct pg_tm tt,
			   *tm = &tt;
```
## Detailed Description
The  function is a core conversion utility that transforms a timestamptz value into a plain timestamp by extracting the local time components and discarding timezone information. The function handles both finite and non-finite timestamp values appropriately.

For finite timestamps, the function performs a two-step conversion process:
1. First, it decomposes the timestamptz into its constituent parts (year, month, day, hour, minute, second, microseconds) using , which applies the session's timezone setting
2. Then, it reconstructs these components into a plain timestamp using , but without any timezone information

For non-finite timestamps (infinity, -infinity), the function simply passes the value through unchanged since these special values don't have timezone-dependent representations.

## Parameters / Member Variables
-  (TimestampTz): The input timestamp with timezone value to be converted

## Dependencies
- Functions called/Symbols referenced:
  -  - macro to check for infinite timestamp values
  -  - decomposes timestamptz to time components with timezone consideration
  -  - reconstructs time components into a plain timestamp
  -  - PostgreSQL error reporting function
  -  - [error](../e/error.md) code specification
  -  - [error](../e/error.md) message formatting
- Called from (representative examples):
  -  - SQL function wrapper
  -  - utility for getting local SQL timestamp
  - Used internally by various timestamp conversion operations

## Notes and Other Information
- This is a static function, meaning it's only accessible within the timestamp.c compilation unit
- The function properly handles error conditions by reporting 'timestamp out of range' errors when conversion fails
- The conversion respects the session's current timezone setting through the  call
- Located in  at lines 6373-6401
- The function is fundamental to PostgreSQL's timezone-aware timestamp handling
- Uses PostgreSQL's standard error reporting mechanism for out-of-range values
- The conversion is timezone-aware but the result is a timezone-naive timestamp