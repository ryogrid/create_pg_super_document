# GetWALInsertionTimeLine

## Location
[src/backend/access/transam/xlog.c:6499-6514](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L6499-L6514)

## Overview
Returns the current timeline ID of a system that is not in recovery, providing the timeline context for WAL operations.

## Definition
TimeLineID GetWALInsertionTimeLine(void)

## Detailed Description
GetWALInsertionTimeLine returns the current timeline ID for systems that are actively writing WAL records and are not in recovery mode. A timeline represents a branch in the WAL stream, typically created during point-in-time recovery operations or when a standby is promoted to primary. This function provides access to the current insertion timeline, which is essential for WAL file naming, replication, and ensuring consistency across different database instances.

The function includes an assertion to ensure it is only called on systems that have completed recovery (SharedRecoveryState must be RECOVERY_STATE_DONE). Since the timeline value cannot change while the system is actively writing WAL (not in recovery), no locking is required to access this value, making the function very lightweight.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - RECOVERY_STATE_DONE
  - Assert (for state validation)
  - XLogCtl (global WAL control structure)
- Called from (representative examples):
  - [WALReadFromBuffers](../W/WALReadFromBuffers.md)
  - [pg_walfile_name_offset](../p/pg_walfile_name_offset.md)
  - [pg_walfile_name](../p/pg_walfile_name.md)
  - READ_REPLICATION_SLOT_COLS
  - [logical_read_xlog_page](../l/logical_read_xlog_page.md)
  - [XLogSendPhysical](../X/XLogSendPhysical.md)
  - [WALAvailability](../W/WALAvailability.md)

## Notes and Other Information
- Should only be used on systems not in recovery (enforced by assertion)
- Timeline values are used for WAL file naming and replication coordination
- No locking required since timeline cannot change during normal WAL writing
- Timeline changes typically occur during recovery or standby promotion
- Located in src/backend/access/transam/xlog.c:6499-6514
- Essential for replication and WAL management operations