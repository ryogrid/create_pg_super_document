# MarkAsPrepared

## Location
[src/backend/access/transam/twophase.c:530-551](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/twophase.c#L530-L551)

## Overview
Marks a GlobalTransaction as fully valid and registers it with the global ProcArray, completing the transition to the prepared state in two-phase commit.

## Definition

```c
static void
MarkAsPrepared(GlobalTransaction gxact, bool lock_held)
```
## Detailed Description
MarkAsPrepared is the final step in preparing a transaction for two-phase commit. It marks the GlobalTransaction as valid, making it visible to other parts of the system, and adds the transaction's PGPROC entry to the global ProcArray. This registration ensures that TransactionIdIsInProgress() will recognize the transaction XID as still running, which is crucial for maintaining proper transaction isolation and visibility. The function provides flexibility in lock management, allowing callers to indicate whether they already hold the required TwoPhaseStateLock.

## Parameters / Member Variables
- : The GlobalTransaction structure to mark as prepared
- : Boolean flag indicating whether the caller already holds TwoPhaseStateLock

## Dependencies
- Functions called/Symbols referenced:
  - [GlobalTransaction](../G/GlobalTransaction.md)
  - [ProcArrayAdd](../P/ProcArrayAdd.md)
  - GetPGProcByNumber
- Called from (representative examples):
  - [EndPrepare](../E/EndPrepare.md)
  - [RecoverPreparedTransactions](../R/RecoverPreparedTransactions.md)

## Notes and Other Information
- This is a static function used internally within the two-phase commit system
- Sets the valid flag to true, making the prepared transaction visible system-wide
- The lock_held parameter allows for optimization when the caller already holds the necessary lock
- Adding to ProcArray is essential for proper transaction visibility and conflict detection
- Must be called after all other preparation steps are complete (including GXactLoadSubxactData if needed)
- The transaction becomes eligible for COMMIT PREPARED or ROLLBACK PREPARED after this call

## Simplified Source

```c
// Simplified version of MarkAsPrepared
static void MarkAsPrepared(GlobalTransaction gxact, bool lock_held) {
    // Acquire lock if caller doesn't already hold it
    if (!lock_held) {
        LWLockAcquire(TwoPhaseStateLock, LW_EXCLUSIVE);
    }

    // Mark the transaction as valid
    Assert(!gxact->valid);
    gxact->valid = true;

    // Release lock if we acquired it
    if (!lock_held) {
        LWLockRelease(TwoPhaseStateLock);
    }

    // Add to global ProcArray for transaction visibility
    ProcArrayAdd(GetPGProcByNumber(gxact->pgprocno));
}
```

Key simplifications made:
- Consolidated lock acquisition and release logic
- Added clear comments for each major operation
- Preserved the conditional locking mechanism for optimization
- Maintained the assertion and ProcArray registration
- Focused on the core state transition from preparing to prepared