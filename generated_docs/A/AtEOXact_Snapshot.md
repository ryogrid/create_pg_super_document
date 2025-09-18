# AtEOXact_Snapshot

## Location
[src/backend/utils/time/snapmgr.c:995-1094](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/time/snapmgr.c#L995-L1094)

## Overview
Comprehensive cleanup function that manages all snapshot-related state at the end of a transaction, handling transaction snapshots, exported snapshots, active snapshots, and global state reset.

## Definition


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
- : Boolean indicating whether this is a commit (true) or abort (false) scenario
- : Boolean indicating whether to reset the process's xmin value (false during normal commit as ProcArrayEndTransaction handles it)

## Dependencies
- Functions called/Symbols referenced:
  - pairingheap_is_empty
  - pairingheap_remove
  - pairingheap_reset
  - ExportedSnapshot
  - unlink
  - [InvalidateCatalogSnapshot](../I/InvalidateCatalogSnapshot.md)
  - ActiveSnapshotElt
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