# LogicalIncreaseRestartDecodingForSlot

## Location
[src/backend/replication/logical/logical.c:1763-1838](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/logical.c#L1763-L1838)

## Overview
Updates the minimal LSN (restart_lsn) needed to replay all uncommitted transactions at a given current_lsn for logical replication slots, taking effect only after the client confirms receipt of the current_lsn.

## Definition

```c
void
LogicalIncreaseRestartDecodingForSlot(XLogRecPtr current_lsn, XLogRecPtr restart_lsn)
```
## Detailed Description
This function manages the restart LSN for logical replication slots, which represents the minimum WAL position required to restart logical decoding without losing any transaction data. The function implements a careful protocol to ensure that restart LSN updates only take effect after client confirmation, preventing data loss during replication.

The function uses a candidate-based approach where proposed restart LSNs are stored as candidates until the client confirms receipt of the corresponding current LSN. This ensures atomicity and prevents race conditions between LSN updates and client acknowledgments.

Three main scenarios are handled:
1. Direct application when current_lsn is already confirmed as flushed
2. Setting new candidates when no pending candidates exist
3. Rejecting updates when candidates are already pending (to prevent endless updates)

## Parameters / Member Variables
- `current_lsn`: The current WAL position at which the restart LSN should take effect
- `restart_lsn`: The proposed minimal LSN needed to replay all uncommitted transactions
## Dependencies
- Functions called/Symbols referenced:
  - [ReplicationSlot](../R/ReplicationSlot.md) (MyReplicationSlot global)
  - [LogicalConfirmReceivedLocation](LogicalConfirmReceivedLocation.md)
  - SpinLockAcquire/SpinLockRelease
  - elog (DEBUG1 logging)
- Called from (representative examples):
  - [SnapBuildProcessRunningXacts](../S/SnapBuildProcessRunningXacts.md)

## Notes and Other Information
- Similar to LogicalIncreaseXminForSlot but operates on restart LSN instead of xmin
- Uses spinlock protection for thread-safe access to slot data
- Includes extensive debug logging for troubleshooting replication issues
- The candidate mechanism prevents scenarios where slow client acknowledgments could cause endless LSN update attempts
- Only updates restart LSN when the new value is actually higher than the current one

## Simplified Source

```c
void
LogicalIncreaseRestartDecodingForSlot(XLogRecPtr current_lsn, XLogRecPtr restart_lsn)
{
    bool updated_lsn = false;
    ReplicationSlot *slot;

    slot = MyReplicationSlot;
    Assert(slot != NULL);
    Assert(restart_lsn != InvalidXLogRecPtr);
    Assert(current_lsn != InvalidXLogRecPtr);

    SpinLockAcquire(&slot->mutex);

    // Don't move restart_lsn backwards
    if (restart_lsn <= slot->data.restart_lsn) {
        SpinLockRelease(&slot->mutex);
        return;
    }

    // If current LSN is already confirmed, apply restart_lsn immediately
    if (current_lsn <= slot->data.confirmed_flush) {
        slot->candidate_restart_valid = current_lsn;
        slot->candidate_restart_lsn = restart_lsn;
        SpinLockRelease(&slot->mutex);
        updated_lsn = true;
    }
    // Set new candidate if no pending candidate exists
    else if (slot->candidate_restart_valid == InvalidXLogRecPtr) {
        slot->candidate_restart_valid = current_lsn;
        slot->candidate_restart_lsn = restart_lsn;
        SpinLockRelease(&slot->mutex);

        elog(DEBUG1, "got new restart lsn %X/%X at %X/%X",
             LSN_FORMAT_ARGS(restart_lsn),
             LSN_FORMAT_ARGS(current_lsn));
    } else {
        // Candidate already pending - log rejection for debugging
        XLogRecPtr candidate_restart_lsn = slot->candidate_restart_lsn;
        XLogRecPtr candidate_restart_valid = slot->candidate_restart_valid;
        XLogRecPtr confirmed_flush = slot->data.confirmed_flush;
        SpinLockRelease(&slot->mutex);

        elog(DEBUG1, "failed to increase restart lsn: proposed %X/%X, after %X/%X, "
                     "current candidate %X/%X, current after %X/%X, flushed up to %X/%X",
             LSN_FORMAT_ARGS(restart_lsn), LSN_FORMAT_ARGS(current_lsn),
             LSN_FORMAT_ARGS(candidate_restart_lsn), LSN_FORMAT_ARGS(candidate_restart_valid),
             LSN_FORMAT_ARGS(confirmed_flush));
    }

    // Apply candidate immediately if already confirmed
    if (updated_lsn)
        LogicalConfirmReceivedLocation(slot->data.confirmed_flush);
}
```