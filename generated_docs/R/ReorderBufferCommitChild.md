# ReorderBufferCommitChild

## Location
src/backend/replication/logical/reorderbuffer.c: 1215 - 1256

## Overview
Associates a subtransaction with its top-level transaction at commit time and finalizes the subtransaction metadata.

## Definition
```c
void ReorderBufferCommitChild(ReorderBuffer *rb, TransactionId xid, TransactionId subxid, XLogRecPtr commit_lsn, XLogRecPtr end_lsn)
```

## Detailed Description
This function is called during the commit process of a subtransaction to establish the final association between the subtransaction and its parent top-level transaction. It sets the final LSN and end LSN for the subtransaction, then ensures the subtransaction is properly assigned as a child of the top-level transaction.

The function includes an optimization to avoid unnecessary work - if the subtransaction contains no changes (i.e., ReorderBufferTXNByXid returns NULL), the function returns early since there is nothing to commit or associate.

After setting the LSN metadata, the function calls ReorderBufferAssignChild to establish the parent-child relationship, which is implemented as a no-op if the assignment has already been done previously.

## Parameters / Member Variables
- `rb`: The reorder buffer instance managing the transactions
- `xid`: The transaction ID of the top-level parent transaction  
- `subxid`: The transaction ID of the subtransaction being committed
- `commit_lsn`: The LSN at which this subtransaction commits
- `end_lsn`: The end LSN for this subtransaction

## Dependencies
- Functions called/Symbols referenced:
  - [ReorderBufferTXNByXid](ReorderBufferTXNByXid.md) (retrieves transaction by XID)
  - ReorderBufferAssignChild (assigns subtransaction to parent)
  - [ReorderBuffer](ReorderBuffer.md) (reorder buffer structure type)
  - [ReorderBufferTXN](ReorderBufferTXN.md) (transaction structure type)
- Called from (representative examples):
  - [DecodeCommit](../D/DecodeCommit.md) (during commit record processing)
  - [DecodePrepare](../D/DecodePrepare.md) (during prepare record processing)

## Notes and Other Information
- This is a public function in the reorder buffer API, callable from decode.c
- The function performs early exit optimization if the subtransaction has no changes
- No further changes should be added to the subtransaction after this function is called
- The function handles both the metadata finalization and parent-child relationship establishment
- This is part of PostgreSQLs logical replication infrastructure for managing transaction hierarchies during WAL decoding