# ReportSlotInvalidation

## Location
[src/backend/replication/slot.c:1477-1542](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/slot.c#L1477-L1542)

## Overview
Reports replication slot invalidation events by logging detailed error messages based on the specific invalidation cause.

## Definition
static void ReportSlotInvalidation(ReplicationSlotInvalidationCause cause, bool terminating, int pid, NameData slotname, XLogRecPtr restart_lsn, XLogRecPtr oldestLSN, TransactionId snapshotConflictHorizon)

## Detailed Description
This static helper function generates comprehensive log messages when replication slots are invalidated. It formats different error messages depending on the invalidation cause:

- RS_INVAL_WAL_REMOVED: Reports when a slot's restart_lsn exceeds available WAL, showing the exact byte difference and suggesting max_slot_wal_keep_size adjustment
- RS_INVAL_HORIZON: Reports conflicts with transaction ID horizons for logical slots
- RS_INVAL_WAL_LEVEL: Reports issues with wal_level configuration for logical decoding on standby

The function supports two reporting modes: terminating processes that own invalidated slots, or reporting invalidation of unused slots. All messages are logged at LOG level with detailed context for troubleshooting.

## Parameters / Member Variables
- cause: The specific reason for invalidation (ReplicationSlotInvalidationCause enum)
- terminating: Whether this is terminating an active process (true) or invalidating unused slot (false)
- pid: Process ID of the slot owner (if any)
- slotname: Name of the invalidated replication slot
- restart_lsn: The slot's restart LSN that caused the conflict
- oldestLSN: The oldest available LSN for WAL removal cases
- snapshotConflictHorizon: Transaction ID horizon for snapshot conflicts

## Dependencies
- Functions called/Symbols referenced:
  - [initStringInfo](../i/initStringInfo.md)
  - [appendStringInfo](../a/appendStringInfo.md)
  - [appendStringInfoString](../a/appendStringInfoString.md)
  - ngettext
  - LSN_FORMAT_ARGS
  - ereport
  - [errmsg](../e/errmsg.md)
  - [errdetail_internal](../e/errdetail_internal.md)
  - [errhint](../e/errhint.md)
  - NameStr
  - [pfree](../p/pfree.md)
  - pg_unreachable
- Called from (representative examples):
  - [InvalidatePossiblyObsoleteSlot](../I/InvalidatePossiblyObsoleteSlot.md) (twice - for termination and invalidation reporting)

## Notes and Other Information
This function is crucial for PostgreSQL administrators to understand why replication slots are being invalidated. The detailed error messages help in diagnosing replication issues and adjusting configuration parameters like max_slot_wal_keep_size or wal_level appropriately.