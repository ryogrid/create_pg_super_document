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
- : LogicalDecodingContext containing the decoding state and configuration
- : XLogRecordBuffer containing the WAL record being processed
- : xl_xact_parsed_prepare structure containing parsed prepare record data including transaction ID, subtransactions, and timing information

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