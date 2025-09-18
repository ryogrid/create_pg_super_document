# ReorderBufferForget

## Location
src/backend/replication/logical/reorderbuffer.c: 3061 - 3102

## Overview
Forgets the contents of a committed transaction that we are not interested in, while still processing any catalog invalidation messages.

## Definition
```c
void ReorderBufferForget(ReorderBuffer *rb, TransactionId xid, XLogRecPtr lsn)
```

## Detailed Description
ReorderBufferForget handles cleanup of committed transactions whose contents are not of interest to the current logical decoding session. This is significantly different from ReorderBufferAbort() because committed transactions may have modified the catalog, requiring special handling of cache invalidations.

The key distinction is that even though we're not interested in the transaction's data changes, we still need to process any catalog invalidation messages it contains. This ensures that our catalog caches remain consistent with the database state after the transaction's catalog modifications.

The function should only be called at the moment a transaction commit has been read, not earlier, to avoid incomplete transaction recreation by later WAL records.

## Parameters / Member Variables
- `rb`: The ReorderBuffer instance managing the transaction
- `xid`: Transaction ID of the transaction to forget
- `lsn`: Log Sequence Number of the commit record

## Dependencies
- Functions called/Symbols referenced:
  - [ReorderBufferTXNByXid](ReorderBufferTXNByXid.md)
  - rbtxn_is_streamed (for assertion)
  - [ReorderBufferImmediateInvalidation](ReorderBufferImmediateInvalidation.md)
  - [ReorderBufferCleanupTXN](ReorderBufferCleanupTXN.md)
- Called from (representative examples):
  - [DecodeCommit](../D/DecodeCommit.md) (in decode.c)

## Notes and Other Information
- Must be called for subtransactions before the toplevel transaction
- Only allowed to be called when a transaction commit has just been read
- Transactions handled here must not be streamed (enforced by assertion)
- Processes cache invalidations even for uninteresting transactions if they have a base snapshot
- Different from ReorderBufferAbort() due to the committed nature of these transactions
- Ensures catalog cache consistency by processing invalidation messages from catalog-modifying transactions