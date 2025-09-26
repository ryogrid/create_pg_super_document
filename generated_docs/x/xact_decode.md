# xact_decode

## Location
[src/backend/replication/logical/decode.c:201-357](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/decode.c#L201-L357)

## Overview
Handles transaction-related WAL records (RM_XACT_ID) during logical decoding, processing commits, aborts, preparations, and invalidations to maintain transactional consistency in logical replication streams.

## Definition
```c
void xact_decode(LogicalDecodingContext *ctx, XLogRecordBuffer *buf)
```

## Detailed Description
This function is the logical decoding handler for transaction-related WAL records managed by the XACT resource manager (RM_XACT_ID). It processes all transaction lifecycle events that are crucial for maintaining ACID properties during logical replication.

The function handles the complete transaction lifecycle including:
- Transaction commits (both regular and prepared transactions)
- Transaction aborts (both regular and prepared transactions) 
- Transaction preparations for two-phase commits
- Cache invalidation messages that ensure catalog consistency
- Subxact assignments (though these are handled at a higher level)

Key processing logic:
1. Ensures snapshot is fully built before processing any transaction events
2. Parses transaction records to extract relevant transaction IDs and metadata
3. Supports two-phase commit processing when enabled by output plugins
4. Manages cache invalidations for both transactional and immediate processing
5. Coordinates with the reorder buffer for proper transaction ordering

The function is essential for logical replication as it transforms low-level WAL transaction records into high-level logical change events.

## Parameters / Member Variables
- `ctx`: LogicalDecodingContext pointer containing snapshot builder, reorder buffer, output plugin configuration, and decoding state
- `buf`: XLogRecordBuffer pointer containing the current transaction record, including WAL position and record data

## Dependencies
- Functions called/Symbols referenced:
  - [SnapBuildCurrentState](../S/SnapBuildCurrentState.md)
  - XLogRecGetInfo
  - XLogRecGetData
  - XLogRecGetXid
  - [ParseCommitRecord](../P/ParseCommitRecord.md) / ParseAbortRecord / ParsePrepareRecord
  - [FilterPrepare](../F/FilterPrepare.md)
  - [DecodeCommit](../D/DecodeCommit.md) / DecodeAbort / DecodePrepare
  - [ReorderBufferAddInvalidations](../R/ReorderBufferAddInvalidations.md)
  - [ReorderBufferXidSetCatalogChanges](../R/ReorderBufferXidSetCatalogChanges.md)
  - [ReorderBufferImmediateInvalidation](../R/ReorderBufferImmediateInvalidation.md)
  - [ReorderBufferProcessXid](../R/ReorderBufferProcessXid.md)
- Constants used:
  - XLOG_XACT_OPMASK
  - XLOG_XACT_COMMIT, XLOG_XACT_ABORT, XLOG_XACT_PREPARE
  - XLOG_XACT_COMMIT_PREPARED, XLOG_XACT_ABORT_PREPARED
  - XLOG_XACT_ASSIGNMENT, XLOG_XACT_INVALIDATIONS
  - SNAPBUILD_FULL_SNAPSHOT
- Data types used:
  - [xl_xact_commit](xl_xact_commit.md), xl_xact_abort, xl_xact_prepare
  - [xl_xact_parsed_commit](xl_xact_parsed_commit.md), xl_xact_parsed_abort, xl_xact_parsed_prepare
  - [xl_xact_invals](xl_xact_invals.md)
- Called from:
  - Resource manager system via LogicalDecodingProcessRecord (registered in rmgrlist.h)

## Notes and Other Information
- This function is registered as the decode handler for RM_XACT_ID in the resource manager list
- Requires a fully built snapshot (SNAPBUILD_FULL_SNAPSHOT) before processing any transaction events
- Two-phase commit support depends on output plugin capabilities and filtering decisions
- Cache invalidation handling differs between transactional (accumulated until commit) and immediate processing
- Subxact assignments are handled at the LogicalDecodingProcessRecord level, not here
- Prepare transaction processing can potentially deadlock if prepared transactions lock catalog tables exclusively
- Fast-forward mode can skip certain invalidation processing for performance
- The function coordinates closely with the reorder buffer to maintain transaction ordering during logical replication