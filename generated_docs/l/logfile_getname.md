# logfile_getname

## Location
[src/backend/postmaster/syslogger.c:1411-1440](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/syslogger.c#L1411-L1440)

## Overview
Constructs log file names using timestamp information and PostgreSQL's configurable filename pattern, with optional suffix support for different log file types.

## Definition

```c
struct pg_tm *tm;
```
## Detailed Description
The  function generates log file names by combining the configured log directory path with a timestamp-formatted filename pattern. It uses PostgreSQL's  configuration parameter as a strftime pattern to create time-based filenames. The function also supports optional suffixes (like ".csv" or ".json") for different log formats, automatically replacing any existing ".log" extension in the pattern. The result is a dynamically allocated string that must be freed by the caller.

## Parameters / Member Variables
- : PostgreSQL time value used to generate the timestamp portion of the filename
- : Optional string suffix to append to the filename (e.g., ".csv", ".json"). If provided, replaces any existing ".log" extension

## Dependencies
- Functions called/Symbols referenced:
  -  - PostgreSQL memory allocation function
  -  - Standard C formatted string function
  -  - PostgreSQL's strftime implementation for timestamp formatting
  -  - PostgreSQL's localtime conversion function
  -  - Standard C string length function
  -  - Standard C string comparison function
  -  - Safe string copy function
- Global variables used:
  -  - Configured log directory path
  -  - Configured filename pattern
  -  - Configured timezone for log timestamps
- Called from (representative examples):
  -  - For initial log file creation
  -  - During logger startup
  -  - During log rotation

## Notes and Other Information
- Returns a palloc'd string that must be freed by the caller
- The  pattern supports all standard strftime format specifiers
- Suffix replacement logic specifically targets ".log" extensions, preserving other extensions
- Uses  as the maximum path length limit
- The function handles timezone conversion using the configured 
- Essential for creating consistent, time-based log file naming across all PostgreSQL log destinations