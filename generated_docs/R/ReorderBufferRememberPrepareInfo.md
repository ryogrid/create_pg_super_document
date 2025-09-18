# ReorderBufferRememberPrepareInfo

## Location
src/backend/replication/logical/reorderbuffer.c: 2799 - 2826

## Overview
Records prepare information for a two-phase transaction to be used later during commit prepared processing.

## Definition
```c
bool ReorderBufferRememberPrepareInfo(ReorderBuffer *rb, TransactionId xid,
                                     XLogRecPtr prepare_lsn, XLogRecPtr end_lsn,
                                     TimestampTz prepare_time,
                                     RepOriginId origin_id, XLogRecPtr origin_lsn)
```

## Detailed Description
ReorderBufferRememberPrepareInfo stores essential prepare-time information in the transaction structure for later use during commit prepared or rollback prepared operations. This function is crucial for two-phase commit protocols where prepare and commit/rollback are separate phases. The stored information includes LSN positions, timing data, and replication origin details that must be preserved between the prepare and final commit/rollback phases. The function returns true if successful or false if the transaction is not found.

## Parameters / Member Variables
- `rb`: The ReorderBuffer instance managing transactions
- `xid`: Transaction ID of the transaction being prepared
- `prepare_lsn`: LSN where the prepare record starts
- `end_lsn`: LSN where the prepare record ends
- `prepare_time`: Timestamp when the transaction was prepared
- `origin_id`: Replication origin identifier for cross-cluster replication
- `origin_lsn`: LSN at the replication origin

## Dependencies
- Functions called/Symbols referenced:
  - ReorderBufferTXNByXid
- Called from (representative examples):
  - DecodePrepare

## Notes and Other Information
This function is essential for two-phase commit support in logical replication. The stored prepare information is later retrieved and used by ReorderBufferFinishPrepared when processing COMMIT PREPARED or ROLLBACK PREPARED records. The function updates the transaction structure fields including final_lsn, end_lsn, prepare_time, origin_id, and origin_lsn to preserve the prepare-time state.