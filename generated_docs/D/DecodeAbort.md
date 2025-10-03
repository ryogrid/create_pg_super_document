# DecodeAbort

## Location
[src/backend/replication/logical/decode.c:850-905](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/decode.c#L850-L905)

## Overview
DecodeAbort processes ABORT records in PostgreSQL's logical replication, handling transaction rollbacks for both regular transactions and two-phase prepared transactions.

## Definition

```c
static void
DecodeAbort(LogicalDecodingContext *ctx, XLogRecordBuffer *buf,
			xl_xact_parsed_abort *parsed, TransactionId xid,
			bool two_phase)
```
## Detailed Description
DecodeAbort handles the abortion of transactions in logical replication by processing WAL abort records. It supports both regular transaction aborts and rollbacks of prepared transactions in two-phase commit scenarios. The function extracts abort information from the WAL record, determines whether the transaction should be processed or skipped, and appropriately handles the rollback through the reorder buffer.

For two-phase transactions, it calls ReorderBufferFinishPrepared with a false commit flag to indicate rollback. For regular transactions, it directly calls ReorderBufferAbort for both the main transaction and all its subtransactions. The function also handles origin tracking for logical replication scenarios involving multiple nodes.

## Parameters / Member Variables
- `*ctx`: LogicalDecodingContext containing the decoding state and configuration
- `*buf`: XLogRecordBuffer containing the WAL record being processed
- `*parsed`: xl_xact_parsed_abort structure containing parsed abort record data including transaction timing and subtransaction information
- `xid`: TransactionId of the transaction being aborted
- `two_phase`: Boolean indicating whether this is aborting a prepared transaction (rollback prepared) or a regular transaction abort
## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetOrigin
  - [DecodeTXNNeedSkip](DecodeTXNNeedSkip.md)
  - [ReorderBufferFinishPrepared](../R/ReorderBufferFinishPrepared.md)
  - [ReorderBufferAbort](../R/ReorderBufferAbort.md)
  - [UpdateDecodingStats](../U/UpdateDecodingStats.md)
- Called from (representative examples):
  - [xact_decode](../x/xact_decode.md)

## Notes and Other Information
- Handles both regular transaction aborts and rollback prepared operations for two-phase commit
- Processes origin information when available (XACT_XINFO_HAS_ORIGIN flag) for multi-node logical replication scenarios
- Ensures all subtransactions are properly aborted before handling the main transaction
- Critical for maintaining transaction consistency during logical replication failures and rollbacks
- Updates decoding statistics after processing the abort to track replication progress