# GetCurrentStatementStartTimestamp

## Location
[src/backend/access/transam/xact.c:876-887](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xact.c#L876-L887)

## Overview
Returns the timestamp marking the start of the current SQL statement execution, providing a consistent timestamp reference for statement-level operations.

## Definition

```c
TimestampTz
GetCurrentStatementStartTimestamp(void)
```
## Detailed Description
This function returns the value stored in the global variable `stmtStartTimestamp`, which represents the timestamp when the current SQL statement began execution. This timestamp is set once at the beginning of each statement via `SetCurrentStatementStartTimestamp()` and remains constant throughout the statement's execution, ensuring consistent time references within a single statement's processing.

The function is crucial for maintaining temporal consistency in operations that may span multiple function calls within a single statement, such as logging, monitoring, and time-based calculations that need to reference the same base timestamp.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - stmtStartTimestamp (global variable)
- Called from (representative examples):
  - [InitializeParallelDSM](../I/InitializeParallelDSM.md)
  - [StorePreparedStatement](../S/StorePreparedStatement.md)
  - [check_log_duration](../c/check_log_duration.md)
  - [pgstat_report_activity](../p/pgstat_report_activity.md)
  - [statement_timestamp](../s/statement_timestamp.md)
  - [CreatePortal](../C/CreatePortal.md)

## Notes and Other Information
- The function is a simple accessor that returns the globally stored statement start timestamp
- The timestamp is set by `SetCurrentStatementStartTimestamp()` at the beginning of statement execution
- In parallel workers, the timestamp is provided by the parallel infrastructure via `SetParallelStartTimestamps()`
- This timestamp remains constant throughout the entire statement execution, ensuring consistency for all time-sensitive operations within the statement
- The return type `TimestampTz` includes timezone information

## Simplified Source

```c
// Simplified version of GetCurrentStatementStartTimestamp
TimestampTz GetCurrentStatementStartTimestamp(void) {
    // Return the global statement start timestamp
    return stmtStartTimestamp;
}
```

Key simplifications made:
- Added clear comment explaining the function's purpose
- This function is already at its simplest form - just returns a global variable
- Maintained the essential timestamp accessor functionality