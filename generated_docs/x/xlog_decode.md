# xlog_decode

## Location
[src/backend/replication/logical/decode.c:129-200](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/decode.c#L129-L200)

## Overview
Handles XLOG_ID resource manager records during logical decoding, processing various WAL record types that manage database state transitions and administrative operations.

## Definition
```c
void xlog_decode(LogicalDecodingContext *ctx, XLogRecordBuffer *buf)
```

## Detailed Description
This function is the logical decoding handler for WAL records managed by the XLOG resource manager (RM_XLOG_ID). It processes various types of administrative and control records that affect the overall state of the database system during logical replication.

The function handles critical checkpoint operations, parameter changes, and various control records. Most importantly, it manages snapshot serialization points during checkpoint operations and validates WAL level requirements for logical decoding on standby servers.

Key operations handled:
- Checkpoint records (shutdown, online, redo) with appropriate snapshot serialization
- Parameter changes that could affect logical decoding capability
- Administrative records like NOOP, NEXTOID, SWITCH operations
- Full-page write and backup-related records

The function ensures proper transaction processing by calling ReorderBufferProcessXid for the record's transaction ID, maintaining consistency in the logical replication stream.

## Parameters / Member Variables
- `ctx`: LogicalDecodingContext pointer containing the snapshot builder, reorder buffer, and other decoding state
- `buf`: XLogRecordBuffer pointer containing the current XLOG record being processed, including original position and record data

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetInfo
  - [ReorderBufferProcessXid](../R/ReorderBufferProcessXid.md)  
  - XLogRecGetXid
  - [SnapBuildSerializationPoint](../S/SnapBuildSerializationPoint.md)
  - XLogRecGetData
  - [RecoveryInProgress](../R/RecoveryInProgress.md)
- Constants used:
  - Various XLOG record type constants (XLOG_CHECKPOINT_SHUTDOWN, XLOG_PARAMETER_CHANGE, etc.)
  - XLR_INFO_MASK
  - WAL_LEVEL_LOGICAL
- Data types used:
  - [SnapBuild](../S/SnapBuild.md)
  - [xl_parameter_change](xl_parameter_change.md)
- Called from:
  - Resource manager system via LogicalDecodingProcessRecord (registered in rmgrlist.h)

## Notes and Other Information
- This function is registered as the decode handler for RM_XLOG_ID in the resource manager list
- Special handling for WAL level validation: prevents logical decoding when wal_level is insufficient on the primary
- Checkpoint records trigger snapshot serialization points, which are crucial for consistent logical decoding startup
- Online checkpoints are handled differently from shutdown/recovery checkpoints - they don't require immediate serialization as RUNNING_XACTS records provide restart points
- Many XLOG record types are simply ignored during logical decoding as they don't affect logical replication streams
- Error handling includes assertions and explicit error reporting for parameter validation issues

## Simplified Source

```c
void xlog_decode(LogicalDecodingContext *ctx, XLogRecordBuffer *buf)
{
    SnapBuild *builder = ctx->snapshot_builder;
    uint8 info = XLogRecGetInfo(buf->record) & ~XLR_INFO_MASK;

    // Process transaction for reordering
    ReorderBufferProcessXid(ctx->reorder, XLogRecGetXid(buf->record), buf->origptr);

    switch (info) {
        case XLOG_CHECKPOINT_SHUTDOWN:
        case XLOG_END_OF_RECOVERY:
            // Create serialization point for consistent restart
            SnapBuildSerializationPoint(builder, buf->origptr);
            break;

        case XLOG_CHECKPOINT_ONLINE:
            // Online checkpoints handled via RUNNING_XACTS records
            break;

        case XLOG_PARAMETER_CHANGE:
            // Validate WAL level is sufficient for logical decoding
            xl_parameter_change *xlrec = (xl_parameter_change *) XLogRecGetData(buf->record);
            if (xlrec->wal_level < WAL_LEVEL_LOGICAL) {
                Assert(RecoveryInProgress());
                ereport(ERROR, "logical decoding requires wal_level >= logical on primary");
            }
            break;

        case XLOG_NOOP:
        case XLOG_NEXTOID:
        case XLOG_SWITCH:
        // ... other administrative record types
            // These records don't affect logical replication
            break;

        default:
            elog(ERROR, "unexpected RM_XLOG_ID record type: %u", info);
    }
}
```