# get_formatted_start_time

## Location
[src/backend/utils/error/elog.c:2704-2727](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/error/elog.c#L2704-L2727)

## Overview
Computes and returns a formatted timestamp string representing the backend start time, using a cached approach for efficiency in PostgreSQL's logging system.

## Definition


## Detailed Description
This function provides a formatted timestamp string representing when the current PostgreSQL backend process started (MyStartTime). It implements a lazy initialization pattern with caching - the timestamp is only computed on the first call and stored in a static buffer for subsequent reuse. The function formats the timestamp using the format "%Y-%m-%d %H:%M:%S %Z" (e.g., "2024-01-15 14:30:25 UTC") and respects the configured log_timezone setting. The caching mechanism ensures that multiple logging operations within the same backend session use consistent start time formatting without repeated computation overhead.

## Parameters / Member Variables
- Returns:  - Pointer to static buffer containing formatted start time string

## Dependencies
- Functions called/Symbols referenced:
  - pg_time_t (timestamp type)
  - FORMATTED_TS_LEN (buffer size constant)
  - pg_strftime (PostgreSQL's strftime implementation)
  - [pg_localtime](../p/pg_localtime.md) (PostgreSQL's localtime implementation)
  - MyStartTime (global variable for backend start time)
  - log_timezone (global timezone setting)
  - formatted_start_time (static buffer variable)
- Called from (representative examples):
  - [write_csvlog](../w/write_csvlog.md) (src/backend/utils/error/csvlog.c:148)
  - [log_status_format](../l/log_status_format.md) (src/backend/utils/error/elog.c:3035)
  - [write_jsonlog](../w/write_jsonlog.md) (src/backend/utils/error/jsonlog.c:193)

## Notes and Other Information
- Returns a pointer to a static buffer that should not be modified or freed by the caller
- The cached value can be reset using reset_formatted_start_time() to force recomputation
- Assumes log_timezone is properly initialized before use (typically handled by guc.c)
- Used across multiple logging formats (CSV, JSON, and standard log format) for consistent timestamping
- Part of PostgreSQL's logging infrastructure to provide process start time information in log entries