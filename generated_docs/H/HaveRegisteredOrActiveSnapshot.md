# HaveRegisteredOrActiveSnapshot

## Location
[src/backend/utils/time/snapmgr.c:1624-1648](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/time/snapmgr.c#L1624-L1648)

## Overview
HaveRegisteredOrActiveSnapshot determines whether there are any active or explicitly registered snapshots in the system, excluding the catalog snapshot in certain cases.

## Definition
```c
bool HaveRegisteredOrActiveSnapshot(void)
```

## Detailed Description
This function provides a comprehensive check for snapshot presence by examining both active and registered snapshot states. It serves as a key function for enforcing longer-lived snapshot requirements and ensuring proper snapshot management.

The function implements a multi-tier checking logic:

1. **Active Snapshot Check**: First checks if there's an active snapshot (ActiveSnapshot != NULL)
2. **Catalog Snapshot Special Case**: Handles the catalog snapshot specially - if it's the only registered snapshot, the function returns false, effectively treating the catalog snapshot as not qualifying for "longer-lived" snapshot requirements
3. **Registered Snapshots Check**: Finally checks if there are any registered snapshots in the RegisteredSnapshots heap

This design allows the function to distinguish between meaningful snapshot activity (user transactions, explicit registrations) and automatic catalog maintenance snapshots.

The catalog snapshot exclusion logic is particularly important: the catalog snapshot is automatically managed and can be invalidated at any time, so it shouldn't be considered when checking for longer-lived snapshot requirements.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - ActiveSnapshot (global variable for current active snapshot)
  - CatalogSnapshot (global variable for catalog snapshot)
  - pairingheap_is_singular (checks if RegisteredSnapshots has exactly one element)
  - pairingheap_is_empty (checks if RegisteredSnapshots is empty)
  - RegisteredSnapshots (pairing heap of registered snapshots)
- Called from (representative examples):
  - [init_toast_snapshot](../i/init_toast_snapshot.md) (ensures snapshot availability for TOAST operations)
  - [AssertHasSnapshotForToast](../A/AssertHasSnapshotForToast.md) (assertion for TOAST snapshot requirements)
  - [SnapBuildInitialSnapshot](../S/SnapBuildInitialSnapshot.md) (logical replication snapshot building)

## Notes and Other Information
- Specifically designed for enforcing longer-lived snapshot requirements
- Excludes cached catalog snapshots unless they're explicitly registered or active
- Returns true for any active snapshot, regardless of type
- The catalog snapshot special case prevents spurious "snapshot available" results when only automatic catalog maintenance is occurring
- Critical for TOAST operations and logical replication which require stable snapshot contexts

## Simplified Source

```c
// Simplified version of HaveRegisteredOrActiveSnapshot
bool HaveRegisteredOrActiveSnapshot(void) {
    // Check for active snapshot first
    if (ActiveSnapshot != NULL)
        return true;

    // Special case: if only catalog snapshot is registered, return false
    // This excludes automatic catalog maintenance from "longer-lived" requirements
    if (CatalogSnapshot != NULL &&
        pairingheap_is_singular(&RegisteredSnapshots))
        return false;

    // Check if any snapshots are registered
    return !pairingheap_is_empty(&RegisteredSnapshots);
}
```

Key simplifications made:
- Preserved the three-tier checking logic
- Added clear comments explaining the catalog snapshot special case
- Maintained the essential pairing heap operations
- Focused on the core snapshot presence detection algorithm
- Simplified the logic flow while preserving functionality