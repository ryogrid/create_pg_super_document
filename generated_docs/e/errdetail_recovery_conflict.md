# errdetail_recovery_conflict

## Location
[src/backend/tcop/postgres.c:2537-2575](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/postgres.c#L2537-L2575)

## Overview
Provides detailed error messages explaining the specific cause of recovery conflicts in hot standby scenarios.

## Definition
```c
static int errdetail_recovery_conflict(ProcSignalReason reason)
```

## Detailed Description
This function translates recovery conflict signal reasons into human-readable error detail messages. It is specifically designed to help users understand why their queries were cancelled during WAL replay in hot standby configurations. The function handles various types of recovery conflicts that can occur when read-only queries on a standby server interfere with the WAL replay process.

Each conflict type has a specific explanation to help users understand the underlying cause and potentially adjust their queries or configuration accordingly.

## Parameters / Member Variables
- `reason`: ProcSignalReason enum value indicating the specific type of recovery conflict that occurred

## Dependencies
- Functions called/Symbols referenced:
  - ProcSignalReason (enumeration of signal reasons)
  - PROCSIG_RECOVERY_CONFLICT_BUFFERPIN (buffer pin conflict)
  - PROCSIG_RECOVERY_CONFLICT_LOCK (relation lock conflict)
  - PROCSIG_RECOVERY_CONFLICT_TABLESPACE (tablespace conflict)
  - PROCSIG_RECOVERY_CONFLICT_SNAPSHOT (snapshot conflict)
  - PROCSIG_RECOVERY_CONFLICT_LOGICALSLOT (logical replication slot conflict)
  - PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK (startup deadlock conflict)
  - PROCSIG_RECOVERY_CONFLICT_DATABASE (database conflict)
  - [errdetail](errdetail.md) (adds detail to error messages)
- Called from (representative examples):
  - [ProcessRecoveryConflictInterrupt](../P/ProcessRecoveryConflictInterrupt.md) (when processing recovery conflict interrupts)

## Notes and Other Information
- Returns 0 in all cases (return value appears to be unused)
- Handles seven different types of recovery conflicts with specific explanatory messages
- Essential for hot standby deployments where understanding conflict sources is crucial
- Provides user-friendly explanations for complex internal recovery conflict scenarios
- Part of PostgreSQL's recovery conflict resolution and error reporting system
- Used exclusively in standby server scenarios during WAL replay conflicts