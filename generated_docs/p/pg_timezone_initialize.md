# pg_timezone_initialize

## Location
src/timezone/pgtz.c: 361 - 382

## Overview
Initializes the timezone library by setting both session_timezone and log_timezone to GMT, ensuring valid timezone values are available before GUC variable initialization begins.

## Definition
```c
void pg_timezone_initialize(void)
```

## Detailed Description
This function serves as an early initialization step for PostgreSQL's timezone system. It is specifically designed to be called before GUC (Grand Unified Configuration) variable initialization begins. The primary purpose is to ensure that log_timezone has a valid value before any logging GUC variables could become set to values that require elog.c to provide timestamps.

The function sets both session_timezone and log_timezone to GMT using pg_tzset("GMT"). GMT is chosen because it can be interpreted without reference to the filesystem, which is important since PGSHAREDIR location may not yet be known, particularly in EXEC_BACKEND subprocess environments.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - [pg_tzset](pg_tzset.md) (loads the GMT timezone)
- Global variables modified:
  - session_timezone (set to GMT timezone object)
  - log_timezone (set to GMT timezone object)
- Called from (representative examples):
  - [InitializeGUCOptions](../I/InitializeGUCOptions.md) (in guc.c during system initialization)

## Notes and Other Information
- Must be called before GUC variable initialization to prevent logging issues
- Uses GMT specifically because it doesn't require filesystem access
- Corresponds to the bootstrap default values defined in guc_tables.c
- Critical for EXEC_BACKEND subprocess environments where PGSHAREDIR is unknown
- Ensures that timestamp-requiring log operations have valid timezone context
- Both session and log timezones are set to the same initial value for consistency