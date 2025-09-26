# str_time

## Location
[src/backend/access/transam/xlog.c:5154-5168](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L5154-L5168)

## Overview
A utility function that converts a PostgreSQL timestamp to a human-readable string representation using the configured log timezone.

## Definition

```c
static char *
str_time(pg_time_t tnow)
```
## Detailed Description
str_time is a static utility function within the XLOG subsystem that provides formatted timestamp strings for logging and diagnostic purposes. The function converts a pg_time_t timestamp into a readable string format using the system's log_timezone setting.

The function uses a static buffer approach for string storage, which means the returned string is valid until the next call to str_time. This design pattern is commonly used in PostgreSQL for simple utility functions where the caller immediately uses the result.

The timestamp format follows the ISO-style pattern "YYYY-MM-DD HH:MM:SS TZ" which provides both date and time information along with timezone designation, making it suitable for log entries and diagnostic output.

## Parameters / Member Variables
- : A pg_time_t value representing the timestamp to be formatted into a string

## Dependencies
- Functions called/Symbols referenced:
  - [pg_strftime](../p/pg_strftime.md): PostgreSQL's timezone-aware strftime implementation
  - [pg_localtime](../p/pg_localtime.md): Converts timestamp to local time using specified timezone
  - log_timezone: Global variable containing the timezone for log messages

- Called from (representative examples):
  - [StartupXLOG](../S/StartupXLOG.md): Used multiple times for logging recovery progress and checkpoint information
  - RefreshXLogWriteResult: Used for logging WAL write status

## Notes and Other Information
- Uses a static buffer of 128 characters, which is sufficient for the timestamp format
- The function is not thread-safe due to the static buffer usage
- Returns a pointer to internal static storage that gets overwritten on subsequent calls
- Specifically designed for logging and diagnostic output within the WAL subsystem
- The timestamp format includes timezone information, making it useful for correlating events across different time zones