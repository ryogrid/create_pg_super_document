# feGetCurrentTimestamp

## Location
src/bin/pg_basebackup/streamutil.c: 869 - 887

## Overview
A frontend version of PostgreSQL's GetCurrentTimestamp() function that returns the current timestamp in PostgreSQL's internal TimestampTz format.

## Definition
```c
TimestampTz feGetCurrentTimestamp(void)
```

## Detailed Description
This function provides a frontend implementation of timestamp retrieval since client utilities are not linked with backend code that contains GetCurrentTimestamp(). It uses the system's gettimeofday() function to get the current time and converts it from Unix epoch format to PostgreSQL's internal timestamp format. The conversion involves adjusting for the difference between Unix epoch (1970-01-01) and PostgreSQL epoch (2000-01-01), then converting from seconds to microseconds precision.

## Parameters / Member Variables
- No parameters (void function)
- Returns: TimestampTz representing current timestamp in PostgreSQL format

## Dependencies
- Functions called/Symbols referenced:
  - gettimeofday
  - SECS_PER_DAY
  - UNIX_EPOCH_JDATE
  - POSTGRES_EPOCH_JDATE
  - USECS_PER_SEC
- Called from (representative examples):
  - [StreamLogicalLog](../S/StreamLogicalLog.md) (multiple calls in pg_recvlogical.c)
  - [flushAndSendFeedback](flushAndSendFeedback.md)
  - [HandleCopyStream](../H/HandleCopyStream.md)
  - [ProcessKeepaliveMsg](../P/ProcessKeepaliveMsg.md)

## Notes and Other Information
- Frontend equivalent of backend's GetCurrentTimestamp() function
- Converts between Unix timestamp format and PostgreSQL's internal timestamp format
- Uses microsecond precision consistent with PostgreSQL's timestamp handling
- Essential for timing operations in PostgreSQL client utilities like pg_basebackup and pg_recvlogical
- The epoch conversion accounts for PostgreSQL using year 2000 as its epoch instead of Unix's 1970