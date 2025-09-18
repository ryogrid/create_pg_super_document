# standby_decode

## Location
src/backend/replication/logical/decode.c: 358 - 403

## Overview
Handles standby-related WAL records (RM_STANDBY_ID) during logical decoding, primarily processing running transaction snapshots and managing transaction cleanup for consistent logical replication.

## Definition
```c
void standby_decode(LogicalDecodingContext *ctx, XLogRecordBuffer *buf)
```

## Detailed Description
This function is the logical decoding handler for standby-related WAL records managed by the STANDBY resource manager (RM_STANDBY_ID). It processes records that are primarily generated on the primary server to help standby servers maintain consistent views of transaction states.

The function's primary responsibility is handling RUNNING_XACTS records, which contain snapshots of all currently running transactions at specific points in time. These records are crucial for:

1. **Snapshot Building**: Updates the snapshot builder with current transaction state information
2. **Transaction Cleanup**: Aborts tracking of old transactions that are no longer relevant
3. **Consistent Restart Points**: Provides safe points where logical decoding can restart

The RUNNING_XACTS processing includes information about all active transactions, including prepared transactions, which is more comprehensive than checkpoint records that only track non-prepared transactions.

Other record types like STANDBY_LOCK and INVALIDATIONS are present but don't require specific processing during logical decoding, as they are handled elsewhere in the system.

## Parameters / Member Variables
- `ctx`: LogicalDecodingContext pointer containing the snapshot builder, reorder buffer, and other decoding state components
- `buf`: XLogRecordBuffer pointer containing the current standby record, including WAL position and record data

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetInfo
  - XLogRecGetXid
  - ReorderBufferProcessXid
  - XLogRecGetData
  - SnapBuildProcessRunningXacts
  - ReorderBufferAbortOld
- Constants used:
  - XLR_INFO_MASK
  - XLOG_RUNNING_XACTS
  - XLOG_STANDBY_LOCK
  - XLOG_INVALIDATIONS
- Data types used:
  - SnapBuild
  - xl_running_xacts
- Called from:
  - Resource manager system via LogicalDecodingProcessRecord (registered in rmgrlist.h)

## Notes and Other Information
- This function is registered as the decode handler for RM_STANDBY_ID in the resource manager list
- RUNNING_XACTS records are the most important for logical decoding, providing comprehensive transaction state information
- The function performs crucial cleanup by aborting old transactions via ReorderBufferAbortOld, preventing unbounded memory usage
- STANDBY_LOCK records are ignored during logical decoding as they don't affect logical replication streams
- INVALIDATIONS records are handled at the transaction level (XLOG_XACT_INVALIDATIONS) rather than here
- The oldestRunningXid from RUNNING_XACTS records determines which old transactions can be safely discarded
- These records provide more complete information than shutdown checkpoints since they include prepared transactions
- Critical for establishing safe restart points during logical replication setup and recovery