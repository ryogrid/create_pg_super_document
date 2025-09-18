# GetCurrentTimestamp

## Location
src/backend/utils/adt/timestamp.c: 1654 - 1671

## Overview
Retrieves the current operating system time with full precision from the gettimeofday() system call, converting it to PostgreSQL's internal TimestampTz format.

## Definition
```c
TimestampTz GetCurrentTimestamp(void)
```

## Detailed Description
The `GetCurrentTimestamp` function is a core PostgreSQL utility function that obtains the current system time using the `gettimeofday()` system call and converts it to PostgreSQL's internal `TimestampTz` representation. This function provides microsecond precision timing and handles the conversion between Unix epoch time and PostgreSQL's epoch time.

The function performs several key operations:
1. Calls `gettimeofday()` to get the current system time in Unix format (seconds since January 1, 1970)
2. Converts from Unix epoch to PostgreSQL epoch (January 1, 2000) by subtracting the difference in days
3. Converts the result to microseconds and adds the microsecond component from `gettimeofday()`
4. Returns the result as a `TimestampTz` value

This function is widely used throughout the PostgreSQL codebase for timing operations, transaction timestamps, logging, monitoring, and other time-sensitive operations.

## Parameters / Member Variables
This function takes no parameters.

**Return value:**
- `TimestampTz result`: Current timestamp in PostgreSQL's internal format with microsecond precision

## Dependencies
- Functions called/Symbols referenced:
  - `gettimeofday`: System call to get current time
  - `POSTGRES_EPOCH_JDATE`: Constant for PostgreSQL epoch start date
  - `UNIX_EPOCH_JDATE`: Constant for Unix epoch start date  
  - `SECS_PER_DAY`: Constant for seconds per day conversion
  - `USECS_PER_SEC`: Constant for microseconds per second conversion

- Called from (representative examples):
  - `[clock_timestamp](../c/clock_timestamp.md)`: SQL function for current timestamp
  - `[SetCurrentStatementStartTimestamp](../S/SetCurrentStatementStartTimestamp.md)`: Transaction timing
  - `[StartTransaction](../S/StartTransaction.md)`: Transaction management
  - `[CreateCheckPoint](../C/CreateCheckPoint.md)`: WAL checkpoint operations
  - `[pgstat_report_activity](../p/pgstat_report_activity.md)`: Statistics reporting
  - `[WalSndLoop](../W/WalSndLoop.md)`: WAL sender operations
  - Many other timing-critical operations throughout PostgreSQL

## Notes and Other Information
- Provides microsecond precision timing using `gettimeofday()` system call
- Handles epoch conversion between Unix time (1970) and PostgreSQL time (2000)
- Used extensively throughout PostgreSQL for precise timing measurements
- Thread-safe system call providing consistent timing across the system
- Critical function for transaction timing, WAL operations, statistics, and monitoring
- The function is defined in `src/backend/utils/adt/timestamp.c` at lines 1654-1671
- Returns `TimestampTz` which includes timezone information and microsecond precision