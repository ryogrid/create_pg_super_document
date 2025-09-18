# time_t_to_timestamptz

## Location
src/backend/utils/adt/timestamp.c: 1811 - 1832

## Overview
time_t_to_timestamptz is a conversion function that transforms a Unix time_t value into PostgreSQL's internal TimestampTz representation.

## Definition
```c
TimestampTz time_t_to_timestamptz(pg_time_t tm)
```

## Detailed Description
This function converts Unix timestamps (time_t values) to PostgreSQL's internal TimestampTz format. PostgreSQL does not use time_t internally, but this function is provided for situations where external time values need to be interpreted, such as file system stat(2) results. The conversion involves adjusting for the different epoch bases (Unix epoch vs PostgreSQL epoch) and converting from second precision to microsecond precision. To ensure ABI stability across different platforms where time_t width may vary, the function uses pg_time_t as the parameter type, which is always 64 bits wide.

## Parameters / Member Variables
- `tm`: The Unix timestamp value (pg_time_t) to be converted to PostgreSQL's TimestampTz format

## Dependencies
- Functions called/Symbols referenced:
  - pg_time_t (type definition for platform-independent time values)
  - POSTGRES_EPOCH_JDATE (PostgreSQL epoch date constant)
  - UNIX_EPOCH_JDATE (Unix epoch date constant)
  - SECS_PER_DAY (seconds per day constant)
  - USECS_PER_SEC (microseconds per second constant)
- Called from (representative examples):
  - pg_stat_file (file statistics functions)
  - pg_ls_dir_files (directory listing functions)
  - pg_control_system (control file system information)
  - pg_control_checkpoint (control file checkpoint information)

## Notes and Other Information
- The function adjusts for the difference between Unix epoch (January 1, 1970) and PostgreSQL epoch (January 1, 2000)
- Converts from second precision (time_t) to microsecond precision (TimestampTz)
- Uses pg_time_t instead of time_t to maintain consistent ABI across platforms
- Primarily used when interfacing with system calls that return time_t values
- The conversion formula accounts for the 30-year difference between Unix and PostgreSQL epochs
- Essential for file system operations and system information functions that need to return timestamp data