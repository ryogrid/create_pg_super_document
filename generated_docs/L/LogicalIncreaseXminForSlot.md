# LogicalIncreaseXminForSlot

## Location
[src/backend/replication/logical/logical.c:1695-1762](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/logical.c#L1695-L1762)

## Overview
Sets the required catalog xmin horizon for historic snapshots in the current replication slot, managing the xmin advancement process with proper confirmation handling.

## Definition

```c
void
LogicalIncreaseXminForSlot(XLogRecPtr current_lsn, TransactionId xmin)
```
## Detailed Description
This function manages the catalog xmin horizon for logical replication slots, which is critical for maintaining the visibility of historical data needed for logical decoding. The function implements a two-phase approach where xmin candidates are first proposed and then confirmed when the client acknowledges receipt of the corresponding LSN.

The function handles several scenarios:
1. Prevents regression by not allowing older xmin values
2. Directly applies xmin if the client has already confirmed the LSN
3. Sets a candidate xmin when no previous candidate exists
4. Ensures proper coordination with client acknowledgments

The xmin management is crucial for garbage collection control - it prevents VACUUM from removing tuples that might still be needed for logical decoding of historical transactions.

Key responsibilities include:
1. Validating that the new xmin is actually newer than the current one
2. Managing candidate xmin values that await client confirmation
3. Coordinating with LogicalConfirmReceivedLocation for xmin advancement
4. Providing appropriate logging for debugging purposes
5. Thread-safe manipulation of replication slot data

## Parameters / Member Variables
- : XLogRecPtr indicating the LSN position associated with this xmin requirement
- : TransactionId representing the minimum transaction ID that must be preserved

## Dependencies
- Functions called/Symbols referenced:
  - [ReplicationSlot](../R/ReplicationSlot.md)
  - [TransactionIdPrecedesOrEquals](../T/TransactionIdPrecedesOrEquals.md)
  - LogicalConfirmReceivedLocation
- Called from (representative examples):
  - SnapBuildProcessRunningXacts

## Notes and Other Information
- The function operates on MyReplicationSlot, requiring an active replication slot context
- Uses spinlock protection for thread-safe access to slot data
- Implements a candidate mechanism where xmin changes await client confirmation
- The candidate_xmin_lsn tracks the LSN that needs to be confirmed before applying the candidate xmin
- If current_lsn is already confirmed (less than or equal to confirmed_flush), the xmin can be applied immediately
- Prevents endless waiting by only setting new candidates when no previous candidate is pending
- Includes DEBUG1 logging to help track xmin advancement for debugging purposes
- The function may trigger immediate confirmation via LogicalConfirmReceivedLocation if conditions are met