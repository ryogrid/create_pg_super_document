# GetCurrentStatementStartTimestamp

## Location
src/backend/access/transam/xact.c: 876 - 887

## Overview
Returns the timestamp marking the start of the current SQL statement execution, providing a consistent timestamp reference for statement-level operations.

## Definition


## Detailed Description
This function returns the value stored in the global variable `stmtStartTimestamp`, which represents the timestamp when the current SQL statement began execution. This timestamp is set once at the beginning of each statement via `SetCurrentStatementStartTimestamp()` and remains constant throughout the statement's execution, ensuring consistent time references within a single statement's processing.

The function is crucial for maintaining temporal consistency in operations that may span multiple function calls within a single statement, such as logging, monitoring, and time-based calculations that need to reference the same base timestamp.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - stmtStartTimestamp (global variable)
- Called from (representative examples):
  - InitializeParallelDSM
  - StorePreparedStatement
  - check_log_duration
  - pgstat_report_activity
  - statement_timestamp
  - CreatePortal

## Notes and Other Information
- The function is a simple accessor that returns the globally stored statement start timestamp
- The timestamp is set by `SetCurrentStatementStartTimestamp()` at the beginning of statement execution
- In parallel workers, the timestamp is provided by the parallel infrastructure via `SetParallelStartTimestamps()`
- This timestamp remains constant throughout the entire statement execution, ensuring consistency for all time-sensitive operations within the statement
- The return type `TimestampTz` includes timezone information