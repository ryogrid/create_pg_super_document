# HandleRecoveryConflictInterrupt

## Location
[src/backend/tcop/postgres.c:3062-3073](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/postgres.c#L3062-L3073)

## Overview
HandleRecoveryConflictInterrupt is a signal handler function that sets flags to indicate a specific type of recovery conflict that needs to be processed during the next interrupt check cycle.

## Definition
```c
void HandleRecoveryConflictInterrupt(ProcSignalReason reason)
```

## Detailed Description
HandleRecoveryConflictInterrupt serves as a signal handler for recovery conflict notifications in PostgreSQL's hot standby and replication system. It runs within a SIGUSR1 signal handler context and is responsible for recording that a specific type of recovery conflict has occurred and needs to be handled.

When called, this function sets flags in the RecoveryConflictPendingReasons array to indicate which specific type of recovery conflict is pending, along with general flags to trigger interrupt processing. The actual resolution of the conflict is deferred until the next CHECK_FOR_INTERRUPTS() call, which allows the system to handle the conflict at a safe interruption point rather than immediately within the signal handler.

Recovery conflicts can occur in hot standby scenarios when replay of WAL records conflicts with queries running on the standby server, such as when a replay operation needs to remove data that an active query is still accessing.

## Parameters / Member Variables
- `reason`: A ProcSignalReason enum value indicating the specific type of recovery conflict that occurred

## Dependencies
- Functions called/Symbols referenced:
  - ProcSignalReason (enum type for different signal reasons)
- Global variables used:
  - RecoveryConflictPendingReasons (array indexed by reason to track specific conflict types)
  - RecoveryConflictPending (general flag indicating any recovery conflict is pending)
  - InterruptPending (general interrupt flag)
- Called from (representative examples):
  - [procsignal_sigusr1_handler](../p/procsignal_sigusr1_handler.md) (multiple calls for different conflict types in src/backend/storage/ipc/procsignal.c)

## Notes and Other Information
- This function runs in a SIGUSR1 signal handler context and must be async-signal-safe
- The latch setting is handled by the calling procsignal_sigusr1_handler function
- Recovery conflicts are processed asynchronously - this function only sets flags for later processing
- The actual conflict resolution occurs during CHECK_FOR_INTERRUPTS() calls in the main execution flow
- Different types of recovery conflicts (deadlock detection, tablespace conflicts, database conflicts, etc.) are tracked separately using the reason parameter
- This mechanism allows hot standby systems to gracefully handle conflicts between WAL replay and active queries

## Simplified Source

```c
// Simplified version of HandleRecoveryConflictInterrupt
void HandleRecoveryConflictInterrupt(ProcSignalReason reason) {
    // Mark the specific type of recovery conflict as pending
    RecoveryConflictPendingReasons[reason] = true;

    // Set general flags to trigger interrupt processing
    RecoveryConflictPending = true;
    InterruptPending = true;

    // Note: latch will be set by procsignal_sigusr1_handler
}
```

Key simplifications made:
- Added explanatory comments for each logical step
- Maintained the exact same logic flow as the original
- No actual simplification needed as the function is already very concise and clear
- Function serves as a simple flag-setting mechanism in signal handler context