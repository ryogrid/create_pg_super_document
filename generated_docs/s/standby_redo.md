# standby_redo

## Location
[src/backend/storage/ipc/standby.c:1159-1284](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/standby.c#L1159-L1284)

## Overview
standby_redo is the main WAL record processing function for standby-related operations during recovery, handling lock acquisition, running transaction snapshots, and cache invalidations.

## Definition

```c
structed on the standby and for logical decoding.
 *
 * This is used for Hot Standby as follows:
 *
 * We can move directly to STANDBY_SNAPSHOT_READY at startup if we
 * start from a shutdown checkpoint because we know nothing was running
 * at that time and our recovery snapshot is known empty. In the more
 * typical case of an online checkpoint we need to jump through a few
 * hoops to get a correct recovery snapshot and this requires a two or
 * sometimes a three stage process.
 *
 * The initial snapshot must contain all running xids and all current
 * AccessExclusiveLocks at a point in time on the standby. Assembling
 * that information while the server is running requires many and
 * various LWLocks, so we choose to derive that information piece by
 * piece and then re-assemble that info on the standby. When that
 * information is fully assembled we move to STANDBY_SNAPSHOT_READY.
 *
 * Since locking on the primary when we derive the information is not
 * strict, we note that there is a time window between the derivation and
 * writing to WAL of the derived information. That allows race conditions
 * that we must resolve, since xids and locks may enter or leave the
 * snapshot during that window. This creates the issue that an xid or
 * lock may start *after* the snapshot has been derived yet *before* the
 * snapshot is logged in the running xacts WAL record. We resolve this by
 * starting to accumulate changes at a point just prior to when we derive
 * the snapshot on the primary, then ignore duplicates when we later apply
 * the snapshot from the running xacts record. This is implemented during
 * CreateCheckPoint() where we use the logical checkpoint location as
 * our starting point and then write the running xacts record immediately
 * before writing the main checkpoint WAL record. Since we always start
 * up from a checkpoint and are immediately at our starting point, we
 * unconditionally move to STANDBY_INITIALIZED. After this point we
 * must do 4 things:
 *	* move shared nextXid forwards as we see new xids
 *	* extend the clog and subtrans with each new xid
 *	* keep track of uncommitted known assigned xids
 *	* keep track of uncommitted AccessExclusiveLocks
 *
 * When we see a commit/abort we must remove known assigned xids and locks
 * from the completing transaction. Attempted removals that cannot locate
 * an entry are expected and must not cause an error when we are in state
 * STANDBY_INITIALIZED. This is implemented in StandbyReleaseLocks() and
 * KnownAssignedXidsRemove().
 *
 * Later, when we apply the running xact data we must be careful to ignore
 * transactions already committed, since those commits raced ahead when
 * making WAL entries.
 *
 * The loose timing also means that locks may be recorded that have a
 * zero xid, since xids are removed from procs before locks are removed.
 * So we must prune the lock list down to ensure we hold locks only for
 * currently running xids, performed by StandbyReleaseOldLocks().
 * Zero xids should no longer be possible, but we may be replaying WAL
 * from a time when they were possible.
 *
 * For logical decoding only the running xacts information is needed;
```
## Detailed Description
This function serves as the central dispatcher for processing WAL records of resource manager type RM_STANDBY_ID during hot standby recovery. It handles three main types of standby-related WAL records: XLOG_STANDBY_LOCK records for acquiring AccessExclusiveLocks, XLOG_RUNNING_XACTS records for maintaining transaction state information, and XLOG_INVALIDATIONS records for cache invalidation messages.

The function first extracts the record type information and verifies that the system is in hot standby mode before processing. For lock records, it iterates through each lock specification and acquires the corresponding AccessExclusiveLocks. For running transaction records, it reconstructs the transaction state and applies it to the process array, also taking the opportunity to report statistics. For invalidation records, it processes committed invalidation messages to maintain cache consistency.

## Parameters / Member Variables
- : XLogReaderState pointer containing the WAL record to be processed

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetInfo (extract record type information)
  - XLogRecHasAnyBlockRefs (verify no backup blocks)
  - XLogRecGetData (extract record data)
  - StandbyAcquireAccessExclusiveLock (acquire locks during recovery)
  - ProcArrayApplyRecoveryInfo (apply transaction state information)
  - pgstat_report_stat (report statistics)
  - ProcessCommittedInvalidationMessages (process cache invalidations)
  - elog (error logging)
- Data structures used:
  - xl_standby_locks (lock acquisition record format)
  - xl_running_xacts (running transactions record format)
  - xl_invalidations (invalidation messages record format)
  - RunningTransactionsData (transaction state structure)
- Record types handled:
  - XLOG_STANDBY_LOCK
  - XLOG_RUNNING_XACTS
  - XLOG_INVALIDATIONS
- Called from (representative examples):
  - WAL replay infrastructure (via RM_STANDBY_ID resource manager)

## Notes and Other Information
- Only processes records when standbyState is not STANDBY_DISABLED
- Asserts that standby records never contain backup blocks
- Handles transaction ID overflow scenarios in running transaction records
- Takes advantage of regular XLOG_RUNNING_XACTS records to report statistics
- Panics on unknown operation codes to ensure system integrity
- Critical component of hot standby functionality in PostgreSQL
- Located in src/backend/storage/ipc/standby.c:1159-1284