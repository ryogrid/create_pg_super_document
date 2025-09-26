# LogicalIncreaseRestartDecodingForSlot

## Location
src/backend/replication/logical/logical.c: 1763 - 1838

## Overview
Updates the minimal LSN (restart_lsn) needed to replay all uncommitted transactions at a given current_lsn for logical replication slots, taking effect only after the client confirms receipt of the current_lsn.

## Definition


## Detailed Description
This function manages the restart LSN for logical replication slots, which represents the minimum WAL position required to restart logical decoding without losing any transaction data. The function implements a careful protocol to ensure that restart LSN updates only take effect after client confirmation, preventing data loss during replication.

The function uses a candidate-based approach where proposed restart LSNs are stored as candidates until the client confirms receipt of the corresponding current LSN. This ensures atomicity and prevents race conditions between LSN updates and client acknowledgments.

Three main scenarios are handled:
1. Direct application when current_lsn is already confirmed as flushed
2. Setting new candidates when no pending candidates exist
3. Rejecting updates when candidates are already pending (to prevent endless updates)

## Parameters / Member Variables
- : The current WAL position at which the restart LSN should take effect
- : The proposed minimal LSN needed to replay all uncommitted transactions

## Dependencies
- Functions called/Symbols referenced:
  - ReplicationSlot (MyReplicationSlot global)
  - LogicalConfirmReceivedLocation
  - SpinLockAcquire/SpinLockRelease
  - elog (DEBUG1 logging)
- Called from (representative examples):
  - SnapBuildProcessRunningXacts

## Notes and Other Information
- Similar to LogicalIncreaseXminForSlot but operates on restart LSN instead of xmin
- Uses spinlock protection for thread-safe access to slot data
- Includes extensive debug logging for troubleshooting replication issues
- The candidate mechanism prevents scenarios where slow client acknowledgments could cause endless LSN update attempts
- Only updates restart LSN when the new value is actually higher than the current one