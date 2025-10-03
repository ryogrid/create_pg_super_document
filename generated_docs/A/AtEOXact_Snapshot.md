# AtEOXact_Snapshot

## Location
[src/backend/utils/time/snapmgr.c:995-1094](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/time/snapmgr.c#L995-L1094)

## Overview
Comprehensive cleanup function that manages all snapshot-related state at the end of a transaction, handling transaction snapshots, exported snapshots, active snapshots, and global state reset.

## Definition

```c
void
AtEOXact_Snapshot(bool isCommit, bool resetXmin)
```
## Detailed Description
This function serves as the central cleanup mechanism for the snapshot management system at transaction end. It performs multiple cleanup tasks:

1. **Transaction Snapshot Cleanup**: Removes the FirstXactSnapshot from RegisteredSnapshots without freeing memory (handled by TopTransactionContext)
2. **Exported Snapshot Cleanup**: Unlinks exported snapshot files and removes them from RegisteredSnapshots
3. **Catalog Snapshot Cleanup**: Invalidates any existing catalog snapshot
4. **Validation on Commit**: Issues warnings for any remaining registered or active snapshots during commit
5. **State Reset**: Clears all global snapshot state variables and resets the RegisteredSnapshots heap
6. **Xmin Management**: Optionally calls SnapshotResetXmin() based on the resetXmin parameter

The function handles both commit and abort scenarios, with additional validation during commits to detect potential snapshot leaks.

## Parameters / Member Variables
- `isCommit`: Boolean indicating whether this is a commit (true) or abort (false) scenario
- `resetXmin`: Boolean indicating whether to reset the process's xmin value (false during normal commit as ProcArrayEndTransaction handles it)
## Dependencies
- Functions called/Symbols referenced:
  - pairingheap_is_empty
  - [pairingheap_remove](../p/pairingheap_remove.md)
  - pairingheap_reset
  - [ExportedSnapshot](../E/ExportedSnapshot.md)
  - unlink
  - [InvalidateCatalogSnapshot](../I/InvalidateCatalogSnapshot.md)
  - [ActiveSnapshotElt](ActiveSnapshotElt.md)
  - [SnapshotResetXmin](../S/SnapshotResetXmin.md)
- Called from (representative examples):
  - [CommitTransaction](../C/CommitTransaction.md)
  - [PrepareTransaction](../P/PrepareTransaction.md)
  - [CleanupTransaction](../C/CleanupTransaction.md)
  - IsMVCCSnapshot (via header inclusion)

## Notes and Other Information
- Memory is not explicitly freed as TopTransactionContext handles cleanup
- File unlink failures only generate warnings since transaction is already committed/aborted
- Validation warnings are only issued during commit to detect snapshot management bugs
- The resetXmin parameter prevents double-resetting during normal commit flow
- Critical for preventing snapshot and file descriptor leaks
- Handles exported snapshots by removing their files from the filesystem
- Ensures all global snapshot tracking variables are properly reset
- Works in conjunction with ProcArrayEndTransaction() for xmin management

## Simplified Source

```c
// Simplified version of AtEOXact_Snapshot
void AtEOXact_Snapshot(bool isCommit, bool resetXmin) {
    // Clean up transaction snapshot
    if (FirstXactSnapshot != NULL) {
        // Remove from registered snapshots list
        pairingheap_remove(&RegisteredSnapshots, &FirstXactSnapshot->ph_node);
    }
    FirstXactSnapshot = NULL;

    // Clean up exported snapshots
    if (exportedSnapshots != NIL) {
        foreach(lc, exportedSnapshots) {
            ExportedSnapshot *esnap = (ExportedSnapshot *) lfirst(lc);

            // Remove snapshot file from filesystem
            if (unlink(esnap->snapfile))
                elog(WARNING, "could not unlink file \"%s\"", esnap->snapfile);

            // Remove from registered snapshots
            pairingheap_remove(&RegisteredSnapshots, &esnap->snapshot->ph_node);
        }
        exportedSnapshots = NIL;
    }

    // Invalidate catalog snapshot
    InvalidateCatalogSnapshot();

    // On commit, check for leftover snapshots (debugging)
    if (isCommit) {
        if (!pairingheap_is_empty(&RegisteredSnapshots))
            elog(WARNING, "registered snapshots seem to remain after cleanup");

        // Check for unpopped active snapshots
        for (ActiveSnapshotElt *active = ActiveSnapshot; active != NULL; active = active->as_next)
            elog(WARNING, "snapshot %p still active", active);
    }

    // Reset all global snapshot state
    ActiveSnapshot = NULL;
    OldestActiveSnapshot = NULL;
    pairingheap_reset(&RegisteredSnapshots);
    CurrentSnapshot = NULL;
    SecondarySnapshot = NULL;
    FirstSnapshotSet = false;

    // Reset xmin if requested
    if (resetXmin)
        SnapshotResetXmin();
}
```

Key simplifications made:
- Removed detailed comments explaining memory management rationale
- Simplified variable declarations and moved them closer to usage
- Condensed error handling to focus on core actions
- Reduced verbose condition checking while preserving essential logic
- Streamlined the exported snapshots cleanup loop structure
- Consolidated state reset operations into a clear sequence
- Removed assert statements and detailed error context for clarity