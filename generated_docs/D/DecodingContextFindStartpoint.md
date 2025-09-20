# DecodingContextFindStartpoint

## Location
[src/backend/replication/logical/logical.c:652-695](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/logical.c#L652-L695)

## Overview
Reads from a logical decoding slot until it finds a consistent starting point for extracting changes from the WAL.

## Definition

```c
void
DecodingContextFindStartpoint(LogicalDecodingContext *ctx)
```
## Detailed Description
This function performs the critical initialization phase of logical decoding by scanning the WAL from the slot's restart LSN until it finds a consistent point where logical decoding can begin. It continuously reads WAL records and processes them through LogicalDecodingProcessRecord until DecodingContextReady indicates that a consistent snapshot has been established. Once a consistent starting point is found, it updates the slot's confirmed_flush LSN to mark progress.

The function implements a loop that:
1. Reads WAL records starting from the slot's restart_lsn
2. Processes each record through the logical decoding machinery
3. Checks if the decoding context is ready (consistent snapshot established)
4. Updates the slot's metadata when a consistent point is reached

## Parameters / Member Variables
- : LogicalDecodingContext pointer containing the decoding state, WAL reader, and associated replication slot

## Dependencies
- Functions called/Symbols referenced:
  - [XLogBeginRead](../X/XLogBeginRead.md)
  - [XLogReadRecord](../X/XLogReadRecord.md)
  - [LogicalDecodingProcessRecord](../L/LogicalDecodingProcessRecord.md)
  - DecodingContextReady
  - CHECK_FOR_INTERRUPTS
  - SpinLockAcquire/SpinLockRelease
- Called from (representative examples):
  - [create_logical_replication_slot](../c/create_logical_replication_slot.md)
  - [CreateReplicationSlot](../C/CreateReplicationSlot.md)

## Notes and Other Information
- This function is essential for establishing a consistent starting point for logical replication
- It handles WAL reading errors by throwing ERROR messages
- The function updates both confirmed_flush and two_phase_at LSNs when two-phase commit support is enabled
- Uses DEBUG1 logging to track the search for the starting point
- Contains interrupt checks to allow for query cancellation during potentially long-running operations