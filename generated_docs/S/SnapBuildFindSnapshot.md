# SnapBuildFindSnapshot

## Location
[src/backend/replication/logical/snapbuild.c:1376-1572](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/snapbuild.c#L1376-L1572)

## Overview
Incrementally builds catalog decoding snapshots by processing running transaction records and managing state transitions through the snapshot building phases until reaching consistency.

## Definition

```c
static bool
SnapBuildFindSnapshot(SnapBuild *builder, XLogRecPtr lsn, xl_running_xacts *running)
```
## Detailed Description
SnapBuildFindSnapshot is a complex state machine that manages the incremental building of consistent snapshots for logical replication. It implements a sophisticated algorithm to determine when enough transaction information has been collected to safely decode catalog changes.

**The function operates through several strategies:**

**Strategy A - Immediate Consistency:**
When no transactions are running (oldestRunningXid == nextXid), the function can immediately jump to CONSISTENT state since there are no in-progress transactions to worry about.

**Strategy B - Snapshot Restoration:**
Attempts to restore a previously serialized snapshot from disk if conditions allow (not building full snapshot, not in slot creation, and valid state exists).

**Strategy C - Incremental Building:**
The most complex path involving multiple state transitions:
- **START → BUILDING_SNAPSHOT**: Begins when transactions are running, establishes nextXid as the threshold
- **BUILDING_SNAPSHOT → FULL_SNAPSHOT**: When all originally running transactions finish (oldestRunningXid >= next_phase_at)
- **FULL_SNAPSHOT → CONSISTENT**: When all transactions needing catalog snapshots finish

**Horizon Management:**
The function validates that the xmin horizon is sufficient - if the running xacts record is too old (older than initial_xmin_horizon), it calls SnapBuildWaitSnapshot and continues processing.

Each state transition is logged with detailed information about remaining transactions and progress toward consistency.

## Parameters / Member Variables
- : The SnapBuild context tracking the current snapshot building state
- : Log sequence number of the running xacts record being processed  
- : Pointer to xl_running_xacts record containing current transaction state

## Dependencies
- Functions called/Symbols referenced:
  - TransactionIdIsNormal
  - NormalTransactionIdPrecedes  
  - [TransactionIdPrecedesOrEquals](../T/TransactionIdPrecedesOrEquals.md)
  - [SnapBuildWaitSnapshot](SnapBuildWaitSnapshot.md)
  - [SnapBuildRestore](SnapBuildRestore.md)
  - ereport (with LOG and DEBUG1 levels)
- Called from (representative examples):
  - [SnapBuildProcessRunningXacts](SnapBuildProcessRunningXacts.md)

## Notes and Other Information
- Returns true if internal maintenance/cleanup should be performed using the xl_running_xacts record
- Uses sophisticated logging at LOG level to track snapshot building progress
- The state machine is designed to handle race conditions around transaction starts and commits
- Critical for ensuring logical replication starts from a consistent point where all necessary catalog information is available
- The function carefully manages xmin/xmax boundaries to optimize heap visibility checks
- Horizon validation prevents attempting to build snapshots when required catalog rows may have been vacuumed
- State transitions are one-way and irreversible within a single snapshot building session