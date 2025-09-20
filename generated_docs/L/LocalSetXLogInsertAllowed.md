# LocalSetXLogInsertAllowed

## Location
[src/backend/access/transam/xlog.c:6401-6415](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L6401-L6415)

## Overview
LocalSetXLogInsertAllowed forces XLogInsertAllowed() to return true in the current process only, providing process-specific override capability for WAL insertion permissions.

## Definition

```c
static int
LocalSetXLogInsertAllowed(void)
```
## Detailed Description
LocalSetXLogInsertAllowed is a static function that provides a mechanism to override the global recovery state check for WAL insertion in specific processes. It sets the LocalXLogInsertAllowed variable to 1 (true), which causes XLogInsertAllowed() to return true without checking the global recovery state. This function is essential for certain operations that need to insert WAL records even during recovery phases, such as checkpoint operations or startup processes. The function returns the previous value, allowing callers to restore the original state if needed.

## Parameters / Member Variables
- No parameters (void function)
- Returns: int (the previous value of LocalXLogInsertAllowed)

## Dependencies
- Functions called/Symbols referenced:
  - LocalXLogInsertAllowed (static variable manipulation)
- Called from (representative examples):
  - RefreshXLogWriteResult
  - [StartupXLOG](../S/StartupXLOG.md)
  - [CreateCheckPoint](../C/CreateCheckPoint.md)

## Notes and Other Information
- Static function with internal linkage (file-scope only)
- Allows switching LocalXLogInsertAllowed back to -1 later and re-calling the function
- Returns the previous value to enable state restoration
- Critical for checkpoint and startup operations that must write WAL during recovery
- Used by recovery and checkpoint processes that have special WAL writing privileges
- Located in src/backend/access/transam/xlog.c:6401-6415
- Provides fine-grained control over WAL insertion permissions at the process level