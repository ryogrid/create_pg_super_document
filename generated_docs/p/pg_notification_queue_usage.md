# pg_notification_queue_usage

## Location
src/backend/commands/async.c: 1481 - 1505

## Overview
SQL function that returns the fraction of the notification queue currently occupied as a floating-point value between 0 and 1.

## Definition


## Detailed Description
This function provides a way for SQL queries to monitor the utilization of PostgreSQL's asynchronous notification queue. It calculates and returns the current queue usage as a percentage (0.0 to 1.0). The function first advances the queue tail to ensure accurate measurements by removing notifications that have been processed by all listening backends, then acquires a shared lock on the notification queue to safely read the usage statistics.

## Parameters / Member Variables
- No parameters (uses PostgreSQL's standard PG_FUNCTION_ARGS macro)

## Dependencies
- Functions called/Symbols referenced:
  - [asyncQueueAdvanceTail](../a/asyncQueueAdvanceTail.md) (advances tail to get accurate measurement)
  - LWLockAcquire (acquires NotifyQueueLock in LW_SHARED mode)
  - [asyncQueueUsage](../a/asyncQueueUsage.md) (calculates actual queue usage fraction)
  - LWLockRelease (releases NotifyQueueLock)
  - PG_RETURN_FLOAT8 (returns double as PostgreSQL Datum)
- Called from:
  - Available as SQL function (no direct C callers found)

## Notes and Other Information
- This is exposed as a SQL-callable function for monitoring purposes
- The function ensures thread-safety by acquiring the NotifyQueueLock in shared mode
- Queue tail advancement before measurement prevents reporting inflated usage due to unprocessed notifications
- Returns a double-precision floating-point value representing queue utilization
- Useful for monitoring and alerting on notification queue capacity