# DecodeCommit

## Location
[src/backend/replication/logical/decode.c:679-774](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/decode.c#L679-L774)

## Overview
The `DecodeCommit` function provides consolidated handling for commit records in logical decoding, processing both regular commits and two-phase commits with support for subtransactions and origin tracking.

## Definition
```c
static void DecodeCommit(LogicalDecodingContext *ctx, XLogRecordBuffer *buf, xl_xact_parsed_commit *parsed, TransactionId xid, bool two_phase)
```

## Detailed Description
This function serves as the central commit processing logic for logical decoding, handling the finalization of transactions in the reorder buffer. It processes both single-phase and two-phase commits, managing subtransactions, origin information, and database filtering.

The function performs several key operations:
1. **Origin handling**: Extracts origin LSN and timestamp if present in the commit record
2. **Snapshot management**: Updates the snapshot builder with commit information
3. **Transaction filtering**: Determines if the transaction should be skipped based on database and origin filters
4. **Subtransaction processing**: Handles child transaction commits or cleanup
5. **Commit finalization**: Either processes regular commits or finishes prepared transactions
6. **Statistics updates**: Updates decoding statistics after processing

The two-phase parameter determines the commit path:
- **Regular commits**: Uses `ReorderBufferCommit` for immediate processing
- **Two-phase commits**: Uses `ReorderBufferFinishPrepared` for previously prepared transactions

## Parameters / Member Variables
- `ctx`: LogicalDecodingContext containing the snapshot builder, reorder buffer, and decoding configuration
- `buf`: XLogRecordBuffer containing the commit record with LSN positions
- `parsed`: xl_xact_parsed_commit structure containing parsed commit information including subtransactions, database ID, and origin data
- `xid`: TransactionId of the main transaction being committed
- `two_phase`: Boolean flag indicating whether to process as a two-phase commit

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetOrigin
  - SnapBuildCommitTxn
  - [DecodeTXNNeedSkip](DecodeTXNNeedSkip.md)
  - [ReorderBufferForget](../R/ReorderBufferForget.md)
  - [ReorderBufferCommitChild](../R/ReorderBufferCommitChild.md)
  - [ReorderBufferFinishPrepared](../R/ReorderBufferFinishPrepared.md)
  - [SnapBuildGetTwoPhaseAt](../S/SnapBuildGetTwoPhaseAt.md)
  - [ReorderBufferCommit](../R/ReorderBufferCommit.md)
  - UpdateDecodingStats
- Called from (representative examples):
  - [xact_decode](../x/xact_decode.md) (at line 242)

## Notes and Other Information
- The function is marked `static` as it's an internal helper for commit processing
- Origin information (LSN and timestamp) takes precedence over local transaction timing when present
- Transaction filtering ensures only relevant transactions are processed, with proper cleanup for skipped transactions
- Subtransaction handling preserves the transaction hierarchy during commit processing
- Invalidations are executed even for skipped transactions to maintain catalog consistency
- Two-phase commit support allows for prepared transaction completion
- Statistics are updated after each transaction to track decoding progress
- The function handles both committed and forgotten transactions appropriately