# DecodePrepare

## Location
[src/backend/replication/logical/decode.c:775-849](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/decode.c#L775-L849)

## Overview
DecodePrepare processes PREPARE records in PostgreSQL's logical replication, handling two-phase commit transactions by managing their decoded state and preparing them for eventual commit or abort.

## Definition

```c
static void
DecodePrepare(LogicalDecodingContext *ctx, XLogRecordBuffer *buf,
			  xl_xact_parsed_prepare *parsed)
```
## Detailed Description
DecodePrepare handles the first phase of two-phase commit protocol in logical replication. When a transaction is prepared (but not yet committed), this function processes the WAL record and manages the transaction state in the reorder buffer. Unlike DecodeCommit, it doesn't skip prepare records even when concurrent aborts are detected, because changes may have already been sent to subscribers and need proper cleanup through the prepare-rollback sequence.

The function performs several key operations:
1. Remembers prepare information for potential later use in commit prepared
2. Checks if the system has reached a consistent state for streaming
3. Determines if the transaction should be processed or skipped
4. Handles subtransaction management
5. Triggers the actual prepare operation through the reorder buffer
6. Updates decoding statistics

## Parameters / Member Variables
- `*ctx`: LogicalDecodingContext containing the decoding state and configuration
- `*buf`: XLogRecordBuffer containing the WAL record being processed
- `*parsed`: xl_xact_parsed_prepare structure containing parsed prepare record data including transaction ID, subtransactions, and timing information
## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetOrigin
  - [ReorderBufferRememberPrepareInfo](../R/ReorderBufferRememberPrepareInfo.md)
  - [SnapBuildCurrentState](../S/SnapBuildCurrentState.md)
  - [ReorderBufferSkipPrepare](../R/ReorderBufferSkipPrepare.md)
  - [DecodeTXNNeedSkip](DecodeTXNNeedSkip.md)
  - [ReorderBufferInvalidate](../R/ReorderBufferInvalidate.md)
  - [ReorderBufferCommitChild](../R/ReorderBufferCommitChild.md)
  - [ReorderBufferPrepare](../R/ReorderBufferPrepare.md)
  - [UpdateDecodingStats](../U/UpdateDecodingStats.md)
- Called from (representative examples):
  - [xact_decode](../x/xact_decode.md)

## Notes and Other Information
- The function includes extensive comments explaining why prepare records are not skipped during concurrent aborts, emphasizing the complexity of handling streaming transactions and subscriber consistency
- Uses two-phase commit protocol where transactions can be prepared first, then later committed or aborted
- Critical for maintaining consistency in logical replication scenarios involving prepared transactions
- Part of the logical decoding infrastructure that enables logical replication and change data capture

## Simplified Source

```c
static void DecodePrepare(LogicalDecodingContext *ctx, XLogRecordBuffer *buf,
                         xl_xact_parsed_prepare *parsed) {
    SnapBuild *builder = ctx->snapshot_builder;
    XLogRecPtr origin_lsn = parsed->origin_lsn;
    TimestampTz prepare_time = parsed->xact_time;
    RepOriginId origin_id = XLogRecGetOrigin(buf->record);
    int i;
    TransactionId xid = parsed->twophase_xid;

    // Use origin timestamp if available
    if (parsed->origin_timestamp != 0)
        prepare_time = parsed->origin_timestamp;

    // Remember prepare info for potential commit prepared later
    if (!ReorderBufferRememberPrepareInfo(ctx->reorder, xid, buf->origptr,
                                         buf->endptr, prepare_time, origin_id,
                                         origin_lsn))
        return;

    // Skip if not in consistent state yet
    if (SnapBuildCurrentState(builder) < SNAPBUILD_CONSISTENT) {
        ReorderBufferSkipPrepare(ctx->reorder, xid);
        return;
    }

    // Check if transaction should be skipped
    if (DecodeTXNNeedSkip(ctx, buf, parsed->dbId, origin_id)) {
        ReorderBufferSkipPrepare(ctx->reorder, xid);
        ReorderBufferInvalidate(ctx->reorder, xid, buf->origptr);
        return;
    }

    // Commit all subtransactions
    for (i = 0; i < parsed->nsubxacts; i++) {
        ReorderBufferCommitChild(ctx->reorder, xid, parsed->subxacts[i],
                                buf->origptr, buf->endptr);
    }

    // Execute the prepare operation
    ReorderBufferPrepare(ctx->reorder, xid, parsed->twophase_gid);

    // Update decoding statistics
    UpdateDecodingStats(ctx);
}
```