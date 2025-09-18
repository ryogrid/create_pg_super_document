# InvalidateCatalogSnapshotConditionally

## Location
src/backend/utils/time/snapmgr.c: 443 - 455

## Overview
Conditionally invalidates the catalog snapshot when it's the only active snapshot, preventing it from blocking global xmin advancement during client input waits.

## Definition
```c
void InvalidateCatalogSnapshotConditionally(void)
```

## Detailed Description
InvalidateCatalogSnapshotConditionally implements an optimization for long-lived database connections by conditionally dropping the catalog snapshot when waiting for client input. The function only invalidates the catalog snapshot if three conditions are met: a catalog snapshot exists, no other active snapshot is present, and the catalog snapshot is the only one in the RegisteredSnapshots heap (detected via pairingheap_is_singular). This prevents the catalog snapshot from unnecessarily holding back the global xmin horizon, which could impede garbage collection and transaction visibility calculations.

If other snapshots are active or registered, the catalog snapshot is likely not the oldest and therefore not blocking xmin advancement, so it's preserved for efficiency.

## Parameters / Member Variables
- None (operates on global snapshot state)

## Dependencies
- Functions called/Symbols referenced:
  - pairingheap_is_singular
  - InvalidateCatalogSnapshot
- Called from (representative examples):
  - PostgresMain

## Notes and Other Information
- Optimization for idle connection scenarios
- Prevents unnecessary xmin horizon blocking
- Only invalidates when catalog snapshot would be the sole registered snapshot
- Called when about to wait for client input
- Preserves catalog snapshot when other snapshots are active
- Located in src/backend/utils/time/snapmgr.c:443-455
- Critical for proper garbage collection in long-running sessions