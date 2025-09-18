# get_formatted_log_time

## Location
src/backend/utils/error/elog.c: 2654 - 2691

## Overview
The get_formatted_log_time function computes and returns a formatted timestamp string for PostgreSQL log entries, ensuring consistency across all log destinations.

## Definition
```c
char *get_formatted_log_time(void)
```

## Detailed Description
This function provides a centralized mechanism for generating consistent timestamps across PostgreSQL's various logging outputs (CSV logs, JSON logs, server logs, etc.). Key features include:

1. **Lazy computation**: The timestamp is computed only once per log event and cached in a static buffer
2. **Consistency guarantee**: All log destinations that need the same timestamp get the exact same value
3. **Microsecond precision**: Includes milliseconds in the formatted output for precise timing
4. **Timezone awareness**: Uses the configured log_timezone for proper local time formatting
5. **Memory efficiency**: Returns a pointer to a static buffer rather than allocating memory

The function uses a specific format: "YYYY-MM-DD HH:MM:SS.mmm TZ" where mmm represents milliseconds.

## Parameters / Member Variables
- No parameters (void function)
- Returns: char* pointing to static buffer containing formatted timestamp string

## Dependencies
- Functions called/Symbols referenced:
  - gettimeofday (system call for current time)
  - pg_strftime (PostgreSQL timezone-aware strftime)
  - [pg_localtime](../p/pg_localtime.md) (PostgreSQL timezone-aware localtime)  
  - sprintf, memcpy (standard C library functions)
  - FORMATTED_TS_LEN (constant defining timestamp buffer length)
  - pg_time_t (PostgreSQL time type)
- Called from (representative examples):
  - [write_csvlog](../w/write_csvlog.md)
  - [write_jsonlog](../w/write_jsonlog.md)
  - [log_status_format](../l/log_status_format.md)

## Notes and Other Information
- Uses static variables formatted_log_time[], saved_timeval, and saved_timeval_set for caching
- The formatted_log_time buffer is checked for emptiness (first character '\0') to determine if computation is needed
- Expects log_timezone to be properly initialized by guc.c before use
- The millisecond insertion is done by formatting the base timestamp with spaces, then overwriting specific positions
- Provides exactly millisecond precision by dividing tv_usec by 1000
- Part of PostgreSQL's logging infrastructure, ensuring temporal consistency across different log formats
- Located in src/backend/utils/error/elog.c with other logging utilities
- Returns the same timestamp value for all calls within the same log event processing cycle
- The static buffer approach eliminates memory allocation overhead during logging