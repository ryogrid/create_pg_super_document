# KnownAssignedXidsDisplay

## Location
[src/backend/storage/ipc/procarray.c:5217-5254](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/procarray.c#L5217-L5254)

## Overview
KnownAssignedXidsDisplay is a debugging function that formats and logs the contents of the KnownAssignedXids array for diagnostic purposes.

## Definition

```c
static void
KnownAssignedXidsDisplay(int trace_level)
```
## Detailed Description
This function provides detailed debugging output for the KnownAssignedXids array, which is essential for troubleshooting Hot Standby recovery issues. The function creates a comprehensive debug trace that includes:

1. Individual transaction IDs with their array positions
2. Count of valid entries in the array
3. Overall array metadata (num, tail, head positions)
4. Formatted output showing the state of the KnownAssignedXids structure

The function is designed to be called only within the startup process context, eliminating the need for special locking mechanisms. However, it's noted as being expensive to execute since it processes the entire array and formats extensive output, even if the resulting log message might be discarded based on the current logging level.

The output format shows each valid transaction ID with its array index position, followed by summary statistics about the array's current state.

## Parameters / Member Variables
- : The logging level at which to emit the debug information (e.g., DEBUG1, DEBUG2, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - [ProcArrayStruct](../P/ProcArrayStruct.md)
  - initStringInfo
  - appendStringInfo
  - elog
  - [pfree](../p/pfree.md)
- Called from (representative examples):
  - xc_slow_answer_inc
  - ProcArrayApplyRecoveryInfo
  - KnownAssignedXidsAdd

## Notes and Other Information
- This is a static function accessible only within procarray.c
- Designed specifically for debugging and diagnostic purposes
- Called only within the startup process, so no special locking is required
- Performance warning: The function is expensive to execute and should not be called in performance-critical code paths
- The expense is incurred even if the log message will be discarded due to current log levels
- Uses StringInfo for efficient string building when formatting the output
- Part of PostgreSQL's Hot Standby debugging infrastructure
- Provides valuable insights into the state of known assigned transactions during recovery operations