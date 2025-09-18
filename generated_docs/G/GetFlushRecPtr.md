# GetFlushRecPtr

## Location
src/backend/access/transam/xlog.c: 6478 - 6498

## Overview
Returns the current WAL flush position, representing the last WAL position known to be fsync'd to disk and guaranteed to be durably stored.

## Definition
XLogRecPtr GetFlushRecPtr(TimeLineID *insertTLI)

## Detailed Description
GetFlushRecPtr provides the current flush position in the WAL stream, which represents the last WAL position that has been fsync'd to disk and is therefore guaranteed to be durably stored. This function is critical for ensuring data durability and recovery consistency, as it marks the boundary of what data can be considered safely committed.

The function includes an assertion to ensure it is only called on systems that are not in recovery (SharedRecoveryState must be RECOVERY_STATE_DONE). It refreshes the write result information and optionally returns the current insertion timeline ID if requested. Since the system is actively writing and flushing WAL when this function is called, the timeline cannot be changing, so no locking is required for timeline access.

## Parameters / Member Variables
- : Optional output parameter to receive the current insertion timeline ID (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - RECOVERY_STATE_DONE
  - RefreshXLogWriteResult
  - Assert (for state validation)
  - XLogCtl (global WAL control structure)
  - LogwrtResult (global write result structure)
- Called from (representative examples):
  - [pg_current_wal_flush_lsn](../p/pg_current_wal_flush_lsn.md)
  - [read_local_xlog_page_guts](../r/read_local_xlog_page_guts.md)
  - [GetLatestLSN](GetLatestLSN.md)
  - [pg_logical_slot_get_changes_guts](../p/pg_logical_slot_get_changes_guts.md)
  - [IdentifySystem](../I/IdentifySystem.md)
  - [StartReplication](../S/StartReplication.md)
  - [WalSndWaitForWal](../W/WalSndWaitForWal.md)
  - [XLogSendPhysical](../X/XLogSendPhysical.md)
  - [XLogSendLogical](../X/XLogSendLogical.md)

## Notes and Other Information
- Should only be used on systems not in recovery (enforced by assertion)
- Returns the position that is guaranteed to be durably stored on disk
- Critical for replication, logical decoding, and data durability guarantees
- No locking required for timeline access during normal operation
- Located in src/backend/access/transam/xlog.c:6478-6498
- Widely used by replication and WAL sender processes