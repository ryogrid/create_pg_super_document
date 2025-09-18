# CheckPointMultiXact

## Location
[src/backend/access/transam/multixact.c:2296-2319](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/multixact.c#L2296-L2319)

## Overview
Performs checkpoint operations for the MultiXact subsystem by writing all dirty MultiXact pages to disk.

## Definition


## Detailed Description
CheckPointMultiXact is responsible for ensuring that all MultiXact data is safely written to disk during checkpoint operations. This function is called as part of both regular checkpoints and shutdown checkpoints to guarantee data persistence and recovery consistency.

The function operates by flushing both MultiXact offset and member data structures to disk. The offset data tracks which MultiXact IDs map to which member lists, while the member data contains the actual transaction IDs and their lock modes for each MultiXact.

The write operations may result in sync requests being queued for later processing by ProcessSyncRequests(), which is part of the overall checkpoint mechanism. This ensures that the data is not only written to the OS buffer cache but also synchronized to the physical storage device.

The function includes tracing points for performance monitoring and debugging, allowing administrators to track MultiXact checkpoint timing and behavior.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - TRACE_POSTGRESQL_MULTIXACT_CHECKPOINT_START
  - [SimpleLruWriteAll](../S/SimpleLruWriteAll.md)
  - TRACE_POSTGRESQL_MULTIXACT_CHECKPOINT_DONE
  - MultiXactOffsetCtl
  - MultiXactMemberCtl
- Called from (representative examples):
  - [CheckPointGuts](CheckPointGuts.md)

## Notes and Other Information
- Called during both regular and shutdown checkpoints
- Ensures all dirty MultiXact pages are written to disk for crash recovery
- May trigger sync requests that are processed later in the checkpoint cycle
- Uses SimpleLRU infrastructure for managing page writes
- Includes performance tracing for monitoring checkpoint behavior
- Critical for maintaining MultiXact consistency across system restarts
- Works with two separate data structures: offset control and member control
- The 'true' parameter to SimpleLruWriteAll indicates this is a checkpoint operation