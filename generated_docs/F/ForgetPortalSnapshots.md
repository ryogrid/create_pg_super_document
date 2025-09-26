# ForgetPortalSnapshots

## Location
[src/backend/utils/mmgr/portalmem.c:1256-1293](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/portalmem.c#L1256-L1293)

## Overview
Drops all active snapshots associated with portals during transaction control operations (COMMIT/ROLLBACK) inside procedures, ensuring no snapshots remain active.

## Definition
```c
void ForgetPortalSnapshots(void)
```

## Detailed Description
ForgetPortalSnapshots is a companion function to HoldPinnedPortals that manages snapshot cleanup during transaction control within stored procedures. Like HoldPinnedPortals, this function must be called when initiating COMMIT or ROLLBACK inside a procedure, but it operates on a different aspect of portal management - the snapshots.

The function performs a two-phase operation:
1. First phase: Scans all portals in PortalHashTable and clears their portalSnapshot fields, counting how many portal snapshots were found
2. Second phase: Pops all active snapshots from the snapshot stack, which should correspond exactly to the portal snapshots that were cleared

The function includes a critical validation check to ensure that the number of portal snapshots found matches the number of active snapshots popped, maintaining snapshot stack integrity.

## Parameters / Member Variables
This function takes no parameters but uses local variables:
- `numPortalSnaps`: Counter for portal snapshots found and cleared
- `numActiveSnaps`: Counter for active snapshots popped from the stack

## Dependencies
- Functions called/Symbols referenced:
  - [hash_seq_init](../h/hash_seq_init.md): Initialize hash table iteration
  - [hash_seq_search](../h/hash_seq_search.md): Iterate through hash table entries  
  - [ActiveSnapshotSet](../A/ActiveSnapshotSet.md): Check if active snapshots exist
  - [PopActiveSnapshot](../P/PopActiveSnapshot.md): Remove active snapshot from stack
  - elog: Error logging function
- Data structures referenced:
  - [HASH_SEQ_STATUS](../H/HASH_SEQ_STATUS.md): Hash table iteration status
  - [PortalHashEnt](../P/PortalHashEnt.md): Hash table entry for portals
  - PortalHashTable: Global hash table of all portals
  - [Portal](../P/Portal.md): Portal data structure
- Called from:
  - [_SPI_commit](../S/_SPI_commit.md): SPI commit operation (after HoldPinnedPortals)
  - [_SPI_rollback](../S/_SPI_rollback.md): SPI rollback operation (after HoldPinnedPortals)

## Notes and Other Information
- This function must be called separately from HoldPinnedPortals and only after steps that are likely to fail have completed
- The function does not handle holdSnapshot fields - those are cleaned up later in PreCommit_Portals
- The validation check ensures snapshot stack consistency and prevents snapshot leaks
- Active snapshots are popped in reverse order, but the portal scan cannot guarantee the correct order for direct snapshot management
- This separation from PreCommit_Portals avoids the need to clean up snapshot management in VACUUM and other complex areas