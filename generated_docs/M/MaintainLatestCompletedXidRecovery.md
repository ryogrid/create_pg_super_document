# MaintainLatestCompletedXidRecovery

## Location
[src/backend/storage/ipc/procarray.c:989-1022](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/procarray.c#L989-L1022)

## Overview
MaintainLatestCompletedXidRecovery is the recovery-specific version of MaintainLatestCompletedXid, used during WAL replay to update the global latest completed transaction ID.

## Definition

```c
static void
MaintainLatestCompletedXidRecovery(TransactionId latestXid)
```
## Detailed Description
This function serves the same purpose as MaintainLatestCompletedXid but is specifically designed for use during WAL (Write-Ahead Log) replay in recovery mode. It updates TransamVariables->latestCompletedXid when the provided transaction ID is newer than the current value.

The key difference from the regular version is that during recovery, the latestCompletedXid may not be properly initialized, so this function handles potentially invalid current values. It uses the nextXid as a reference point for creating a proper FullTransactionId comparison, which is safe to access without additional locking during recovery since only the startup process is running.

## Parameters / Member Variables
- `latestXid`: The transaction ID representing a completed transaction discovered during WAL replay
## Dependencies
- Functions called/Symbols referenced:
  - AmStartupProcess
  - [LWLockHeldByMe](../L/LWLockHeldByMe.md)
  - FullTransactionIdIsValid
  - [TransactionIdPrecedes](../T/TransactionIdPrecedes.md)
  - XidFromFullTransactionId
  - [FullXidRelativeTo](../F/FullXidRelativeTo.md)
  - FullTransactionIdIsNormal
- Called from (representative examples):
  - [ProcArrayApplyRecoveryInfo](../P/ProcArrayApplyRecoveryInfo.md)
  - [ExpireTreeKnownAssignedTransactionIds](../E/ExpireTreeKnownAssignedTransactionIds.md)
  - [ExpireOldKnownAssignedTransactionIds](../E/ExpireOldKnownAssignedTransactionIds.md)

## Notes and Other Information
- Only used during WAL replay by the startup process or when not under postmaster
- Handles the case where latestCompletedXid might not be initialized during recovery
- Uses nextXid as a reference for FullTransactionId creation since it's safe to access during recovery
- Must be called while holding ProcArrayLock for consistency
- Essential for maintaining correct transaction visibility during crash recovery
- Part of PostgreSQL's recovery infrastructure for rebuilding transaction state from WAL

## Simplified Source

```c
// Simplified version of MaintainLatestCompletedXidRecovery
static void
MaintainLatestCompletedXidRecovery(TransactionId latestXid)
{
    FullTransactionId current_latest = TransamVariables->latestCompletedXid;
    FullTransactionId reference_xid;

    // Get reference transaction ID for comparison
    // Safe to access nextXid during recovery without additional locking
    reference_xid = TransamVariables->nextXid;

    // Update latestCompletedXid if new transaction is more recent
    // Handle case where current value might be uninitialized during recovery
    if (!FullTransactionIdIsValid(current_latest) ||
        TransactionIdPrecedes(XidFromFullTransactionId(current_latest), latestXid))
    {
        // Convert relative XID to full transaction ID and update global state
        TransamVariables->latestCompletedXid = FullXidRelativeTo(reference_xid, latestXid);
    }
}
```

Key simplifications made:
- Removed detailed assertions for clarity (keeping essential logic checks)
- Used more descriptive variable names (current_latest, reference_xid)
- Added explanatory comments for the main logic steps
- Simplified the conditional check structure for better readability
- Abstracted the core purpose: updating global transaction state during recovery