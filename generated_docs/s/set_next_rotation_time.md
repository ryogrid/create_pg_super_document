# set_next_rotation_time

## Location
[src/backend/postmaster/syslogger.c:1441-1475](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/syslogger.c#L1441-L1475)

## Overview
Calculates and sets the next scheduled time for automatic log file rotation based on the configured rotation age interval.

## Definition

```c
struct pg_tm *tm;
```
## Detailed Description
The  function determines when the next automatic log rotation should occur based on the  configuration parameter. It calculates the next time that is aligned to the configured rotation interval, using the local timezone rather than GMT for alignment. The function performs timezone-aware calculations to ensure consistent rotation times regardless of timezone changes or daylight saving time transitions. If time-based rotation is disabled (), the function returns early without setting a rotation time.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  -  - Standard C function to get current time
  -  - PostgreSQL's timezone-aware localtime conversion
  -  - PostgreSQL time type
  -  - PostgreSQL time structure
  -  - Constant for seconds per minute conversion
- Global variables used:
  -  - Configuration parameter for rotation interval in minutes
  -  - Configured timezone for log operations
  -  - Global variable storing the next scheduled rotation time
- Called from (representative examples):
  -  - During logger initialization and after rotation events
  -  - After completing a rotation to schedule the next one

## Notes and Other Information
- Returns early if  (time-based rotation disabled)
- Performs timezone-aware alignment by adjusting for  (GMT offset)
- Uses modulo arithmetic to align rotation times to interval boundaries
- The "multiple" alignment is interpreted loosely but ensures consistent rotation scheduling
- Critical for maintaining predictable log rotation schedules in production environments
- Handles timezone transitions gracefully by working in local time and converting back to UTC