# SyncRepGetOldestSyncRecPtr

## Location
[src/backend/replication/syncrep.c:660-692](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/syncrep.c#L660-L692)

## Overview
Calculates the oldest (most conservative) Write, Flush, and Apply LSN positions among all synchronous standbys.

## Definition

```c
static void
SyncRepGetOldestSyncRecPtr(XLogRecPtr *writePtr,
						   XLogRecPtr *flushPtr,
						   XLogRecPtr *applyPtr,
						   SyncRepStandbyData *sync_standbys,
						   int num_standbys)
```
## Detailed Description
This function implements the position calculation logic for priority-based synchronous replication in PostgreSQL. It iterates through all synchronous standbys and finds the minimum (oldest) LSN position for each operation type (write, flush, apply).

The function uses a conservative approach where synchronization is only considered complete when ALL synchronous standbys have reached at least the determined position. This ensures data durability across all priority-based sync standbys.

The algorithm compares each standby's positions and retains the smallest valid LSN for each operation type. Invalid LSN positions are properly handled by checking with  before comparison.

## Parameters / Member Variables
- : Output parameter - receives the oldest write LSN position among sync standbys
- : Output parameter - receives the oldest flush LSN position among sync standbys
- : Output parameter - receives the oldest apply LSN position among sync standbys
- : Input array of  structures containing standby positions
- : Number of synchronous standbys in the input array

## Dependencies
- Functions called/Symbols referenced:
  -  - Validates LSN positions before comparison
  -  - Data structure containing standby LSN positions
- Called from:
  -  (src/backend/replication/syncrep.c:108)
  -  (src/backend/replication/syncrep.c:642)

## Notes and Other Information
- Assumes output parameters are initialized to  before calling
- Used specifically for priority-based synchronous replication method
- More efficient than  when calculating oldest positions
- Handles invalid LSN positions gracefully by checking validity before comparison
- Function implements the "wait for all" semantics of priority-based sync replication
- Function location: src/backend/replication/syncrep.c:660-692