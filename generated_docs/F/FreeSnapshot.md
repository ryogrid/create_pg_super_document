# FreeSnapshot

## Location
[src/backend/utils/time/snapmgr.c:630-647](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/time/snapmgr.c#L630-L647)

## Overview
Frees the memory associated with a copied snapshot structure, ensuring proper cleanup of dynamically allocated snapshot resources.

## Definition
```c
static void FreeSnapshot(Snapshot snapshot)
```

## Detailed Description
This function safely deallocates memory for a snapshot structure that was previously allocated with CopySnapshot. It includes several important safety checks to ensure the snapshot is in a valid state for deallocation. The function only accepts copied snapshots (not original snapshots from the snapshot data structures) and verifies that no references to the snapshot remain before freeing the memory.

The function performs validation to ensure:
- No registered references remain (regd_count == 0)
- No active uses remain (active_count == 0) 
- The snapshot was created as a copy (copied flag set)

Since CopySnapshot allocates the snapshot structure and its XID arrays in a single memory block, a single pfree() call deallocates all associated memory.

## Parameters / Member Variables
- `snapshot`: The copied snapshot to free, must have zero reference counts and copied flag set

## Dependencies
- Functions called/Symbols referenced:
  - [pfree](../p/pfree.md) (memory deallocation)
- Called from (representative examples):
  - [PopActiveSnapshot](../P/PopActiveSnapshot.md)
  - [UnregisterSnapshotNoOwner](../U/UnregisterSnapshotNoOwner.md)  
  - [AtSubAbort_Snapshot](../A/AtSubAbort_Snapshot.md)

## Notes and Other Information
- This is a static function in snapmgr.c, not exposed as a public API
- Only works with copied snapshots - original snapshots from snapshot data are not freed this way
- The assertions ensure memory safety by preventing premature deallocation
- Memory layout created by CopySnapshot allows single pfree() to clean up everything
- Called during snapshot cleanup operations when snapshots are no longer needed

## Simplified Source

```c
// Simplified version of FreeSnapshot
static void FreeSnapshot(Snapshot snapshot) {
    // Safety checks: ensure snapshot is safe to free
    Assert(snapshot->regd_count == 0);    // No registered references
    Assert(snapshot->active_count == 0);  // No active uses
    Assert(snapshot->copied);             // Must be a copied snapshot

    // Free the snapshot memory (includes XID arrays due to CopySnapshot layout)
    pfree(snapshot);
}
```

Key simplifications made:
- This function is already very simple as it's just a cleanup function
- Added clear comments explaining each assertion's purpose
- Highlighted that pfree() frees both the structure and XID arrays
- Emphasized the safety checks that prevent memory corruption
- Focused on the essential memory management aspect