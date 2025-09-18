# gistRedoPageReuse

## Location
[src/backend/access/gist/gistxlog.c:376-396](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistxlog.c#L376-L396)

## Overview
Handles WAL replay of GiST page reuse operations by resolving recovery conflicts with concurrent snapshots in hot standby mode.

## Definition
```c
static void gistRedoPageReuse(XLogReaderState *record)
```

## Detailed Description
This function processes the `gistxlogPageReuse` WAL record during recovery to handle page reuse conflicts in hot standby scenarios. When a previously deleted GiST page is reused through the Free Space Map (FSM), this function ensures that any concurrent read-only queries on standby servers that might still reference the old page content are properly handled.

The function's primary purpose is to provide a conflict point for transaction visibility. It doesn't modify any actual page data but instead resolves potential conflicts by ensuring that transactions with snapshots older than the page's deletion XID are aborted or waited for, maintaining consistency between primary and standby servers.

The conflict resolution mirrors the visibility logic used during normal page recycling operations, ensuring that the same exclusion effect is achieved on both primary and standby systems.

## Parameters / Member Variables
- `record`: XLogReaderState pointer containing the WAL record data for the page reuse operation

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetData: Extract WAL record data
  - InHotStandby: Global variable indicating if running in hot standby mode
  - [ResolveRecoveryConflictWithSnapshotFullXid](../R/ResolveRecoveryConflictWithSnapshotFullXid.md): Resolve conflicts with concurrent snapshots
  - [gistxlogPageReuse](gistxlogPageReuse.md): WAL record structure containing reuse information
- Called from:
  - [gist_redo](gist_redo.md): Main GiST WAL redo dispatcher

## Notes and Other Information
- This function only executes meaningful work when `InHotStandby` is true
- The `snapshotConflictHorizon` represents the deleteXid of the reused page
- The conflict resolution mechanism mirrors the `GlobalVisCheckRemovableFullXid(deleteXid)` test in `gistPageRecyclable()`
- This ensures consistency with the `PGPROC->xmin > limitXmin` test used in `GetConflictingVirtualXIDs()`
- The function supports catalog relation awareness through the `isCatalogRel` field for appropriate conflict handling