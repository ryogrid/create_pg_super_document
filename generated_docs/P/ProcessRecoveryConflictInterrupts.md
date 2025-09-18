# ProcessRecoveryConflictInterrupts

## Location
src/backend/tcop/postgres.c: 3232 - 3270

## Overview
ProcessRecoveryConflictInterrupts iterates through all pending recovery conflict types and processes each one individually during interrupt handling in PostgreSQL's hot standby system.

## Definition
```c
static void ProcessRecoveryConflictInterrupts(void)
```

## Detailed Description
ProcessRecoveryConflictInterrupts serves as the main dispatcher for handling recovery conflicts that have been flagged for processing. This function is called from ProcessInterrupts() when RecoveryConflictPending is true, indicating that one or more recovery conflicts need to be resolved.

The function systematically checks each possible recovery conflict type in the RecoveryConflictPendingReasons array, from PROCSIG_RECOVERY_CONFLICT_FIRST to PROCSIG_RECOVERY_CONFLICT_LAST. For each conflict type that is pending, it clears the pending flag and delegates the actual conflict resolution to ProcessRecoveryConflictInterrupt().

The function includes several important safety assertions:
- Ensures that process exit is not in progress (proc_exit_inprogress is false)
- Verifies that interrupt holdoff count is zero (safe to process interrupts)
- Confirms that recovery conflicts are actually pending

This design allows multiple recovery conflicts to be handled in a single interrupt processing cycle while maintaining proper ordering and safety guarantees.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - ProcessRecoveryConflictInterrupt (handles individual conflict types)
  - ProcSignalReason (enum type used for iteration bounds)
  - PROCSIG_RECOVERY_CONFLICT_FIRST (start of conflict reason range)
  - PROCSIG_RECOVERY_CONFLICT_LAST (end of conflict reason range)
- Global variables used:
  - proc_exit_inprogress (asserted to be false for safety)
  - InterruptHoldoffCount (asserted to be zero)
  - RecoveryConflictPending (cleared after processing begins)
  - RecoveryConflictPendingReasons (array tracking specific pending conflicts)
- Called from:
  - ProcessInterrupts (main interrupt processing function)

## Notes and Other Information
- This is a static function, only accessible within the same source file
- The function processes all pending conflicts in a single call, improving efficiency
- Individual conflict flags are cleared before processing to prevent reprocessing
- The function is designed to be called only when it's safe to process interrupts
- Recovery conflicts can be set asynchronously by signal handlers but are processed synchronously here
- The sequential processing ensures that conflicts are handled in a deterministic order
- If ProcessRecoveryConflictInterrupt() throws an ERROR or FATAL, remaining conflicts in the loop may not be processed
- The function is part of PostgreSQL's interrupt handling infrastructure for hot standby systems