# SetCommitTsLimit

## Location
[src/backend/access/transam/commit_ts.c:916-942](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/commit_ts.c#L916-L942)

## Overview
Sets the valid transaction ID range within which commit timestamps can be consulted, ensuring proper bounds checking for commit timestamp queries.

## Definition

```c
void
SetCommitTsLimit(TransactionId oldestXact, TransactionId newestXact)
```
## Detailed Description
SetCommitTsLimit establishes the valid range of transaction IDs for which commit timestamp data can be safely queried. This function is crucial for maintaining data integrity by preventing access to commit timestamp data that may be invalid or have been truncated.

The function operates with careful logic to avoid overwriting existing limits that might be more restrictive:
1. If existing limits are already set (not InvalidTransactionId), it only updates them to be more restrictive
2. For the oldest limit, it moves forward in time (to a newer transaction) if the new value is newer
3. For the newest limit, it moves backward in time (to an older transaction) if the new value is older
4. If no limits are currently set, it establishes both limits from the provided parameters

This conservative approach ensures that the valid range can only shrink, never expand inappropriately, which is critical for data consistency.

## Parameters / Member Variables
- `oldestXact`: The oldest transaction ID for which commit timestamp data should be considered valid
- `newestXact`: The newest transaction ID for which commit timestamp data should be considered valid
## Dependencies
- Functions called/Symbols referenced:
  - [LWLockAcquire](../L/LWLockAcquire.md) (CommitTsLock, LW_EXCLUSIVE)
  - [LWLockRelease](../L/LWLockRelease.md) (CommitTsLock)
  - [TransactionIdPrecedes](../T/TransactionIdPrecedes.md) (for comparing transaction IDs)
  - InvalidTransactionId (constant for invalid transaction ID)
  - Assert (for debugging validation)
  - TransamVariables->oldestCommitTsXid (global variable)
  - TransamVariables->newestCommitTsXid (global variable)

- Called from (representative examples):
  - [BootStrapXLOG](../B/BootStrapXLOG.md) (during database bootstrap)
  - [StartupXLOG](StartupXLOG.md) (during recovery startup)

## Notes and Other Information
- Uses exclusive locking on CommitTsLock to ensure atomicity of limit updates
- The function is conservative: it only makes the valid range more restrictive, never less restrictive
- When existing limits are InvalidTransactionId, both oldest and newest should be invalid (enforced by assertion)
- The function is typically called during startup and recovery operations to establish proper bounds
- Proper limit setting is essential for preventing access to truncated or invalid commit timestamp data
- The function is exported via commit_ts.h for use during bootstrap and recovery processes

## Simplified Source

```c
// Simplified version of SetCommitTsLimit
void SetCommitTsLimit(TransactionId oldestXact, TransactionId newestXact) {
    // Acquire exclusive lock to ensure atomic updates
    LWLockAcquire(CommitTsLock, LW_EXCLUSIVE);

    // Check if limits are already established
    if (TransamVariables->oldestCommitTsXid != InvalidTransactionId) {
        // Update limits conservatively - only make range more restrictive
        if (TransactionIdPrecedes(TransamVariables->oldestCommitTsXid, oldestXact))
            TransamVariables->oldestCommitTsXid = oldestXact;  // Move oldest forward
        if (TransactionIdPrecedes(newestXact, TransamVariables->newestCommitTsXid))
            TransamVariables->newestCommitTsXid = newestXact;  // Move newest backward
    } else {
        // No limits set yet - establish initial limits
        TransamVariables->oldestCommitTsXid = oldestXact;
        TransamVariables->newestCommitTsXid = newestXact;
    }

    // Release lock
    LWLockRelease(CommitTsLock);
}
```

Key simplifications made:
- Removed detailed comments about "future" values and disabled committs for clarity
- Added descriptive inline comments explaining the conservative update logic
- Simplified the conditional logic flow with clearer explanations
- Removed the Assert statement as it's not core to the algorithm
- Made the two-phase logic (existing vs new limits) more explicit with better comments