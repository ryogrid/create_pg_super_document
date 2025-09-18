# timestamptz_to_time_t

## Location
src/backend/utils/adt/timestamp.c: 1833 - 1852

## Overview
timestamptz_to_time_t is a conversion function that transforms PostgreSQL's internal TimestampTz representation into a Unix time_t value.

## Definition
```c
pg_time_t timestamptz_to_time_t(TimestampTz t)
```

## Detailed Description
This function converts PostgreSQL's internal TimestampTz format to Unix timestamps (time_t values). It performs the inverse operation of time_t_to_timestamptz, adjusting for the different epoch bases and converting from microsecond precision to second precision. The function is marginally useful but necessary for certain operations that need to interface with system calls or external libraries expecting time_t values. Like its counterpart, it uses pg_time_t as the return type to ensure consistent ABI across different platforms where time_t width may vary.

## Parameters / Member Variables
- `t`: The PostgreSQL TimestampTz value to be converted to Unix time_t format

## Dependencies
- Functions called/Symbols referenced:
  - pg_time_t (type definition for platform-independent time values)
  - USECS_PER_SEC (microseconds per second constant)
  - POSTGRES_EPOCH_JDATE (PostgreSQL epoch date constant)
  - UNIX_EPOCH_JDATE (Unix epoch date constant)
  - SECS_PER_DAY (seconds per day constant)
- Called from (representative examples):
  - [InitProcessGlobals](../I/InitProcessGlobals.md) (process initialization)
  - [DetermineTimeZoneAbbrevOffsetTS](../D/DetermineTimeZoneAbbrevOffsetTS.md) (timezone offset determination)
  - [timestamptz_to_str](timestamptz_to_str.md) (timestamp string conversion in pg_waldump)

## Notes and Other Information
- Performs the inverse conversion of time_t_to_timestamptz
- Converts from microsecond precision (TimestampTz) to second precision (time_t)
- Adjusts for the 30-year difference between PostgreSQL epoch (January 1, 2000) and Unix epoch (January 1, 1970)
- Uses pg_time_t instead of time_t to maintain consistent ABI across platforms
- Primary use cases include interfacing with system calls and external libraries
- The conversion formula adds back the epoch difference that was subtracted in the reverse conversion
- Essential for operations that need to export PostgreSQL timestamps to external systems