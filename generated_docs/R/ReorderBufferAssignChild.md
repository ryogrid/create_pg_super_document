# ReorderBufferAssignChild

## Location
[src/backend/replication/logical/reorderbuffer.c:1095-1160](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/reorderbuffer.c#L1095-L1160)

## Overview
Establishes a parent-child relationship between transactions by assigning a subtransaction to its parent transaction in the reorder buffer.

## Definition
```c
void ReorderBufferAssignChild(ReorderBuffer *rb, TransactionId xid, TransactionId subxid, XLogRecPtr lsn)
```

## Detailed Description
ReorderBufferAssignChild manages the hierarchical relationship between transactions and their subtransactions within the reorder buffer. This function is called when the system discovers that a transaction ID (subxid) is actually a subtransaction of another transaction (xid).

The function performs several critical operations:
1. Retrieves or creates transaction objects for both the parent and child transactions
2. Handles the case where the subtransaction was previously treated as a top-level transaction and removes it from the top-level list
3. Marks the subtransaction with the RBTXN_IS_SUBXACT flag and sets its toplevel_xid
4. Establishes the parent-child references (subtxn->toptxn points to parent)
5. Adds the subtransaction to the parent's subtxns list
6. Transfers any snapshot from the subtransaction to the parent if needed
7. Validates LSN ordering to maintain consistency

This relationship management is essential for logical replication to handle complex transaction hierarchies correctly and ensure that subtransaction changes are processed as part of their parent transaction's commit.

## Parameters / Member Variables
- `rb`: Pointer to a ReorderBuffer structure managing the transaction relationships
- `xid`: TransactionId of the parent (top-level) transaction
- `subxid`: TransactionId of the subtransaction to be assigned as a child
- `lsn`: XLogRecPtr indicating the LSN position where this relationship was discovered

## Dependencies
- Functions called/Symbols referenced:
  - ReorderBufferTXNByXid (retrieves or creates transaction objects)
  - rbtxn_is_known_subxact (checks if transaction is already marked as subtransaction)
  - dlist_delete (removes subtransaction from top-level list if needed)
  - dlist_push_tail (adds subtransaction to parent's subtxns list)
  - ReorderBufferTransferSnapToParent (transfers snapshot from child to parent)
  - AssertTXNLsnOrder (validates LSN ordering consistency)
- Data structures used:
  - ReorderBuffer
  - ReorderBufferTXN
  - TransactionId
  - XLogRecPtr
- Constants used:
  - RBTXN_IS_SUBXACT (flag marking a transaction as a subtransaction)
- Called from (representative examples):
  - LogicalDecodingProcessRecord (at src/backend/replication/logical/decode.c:107)
  - ReorderBufferCommitChild (at src/backend/replication/logical/reorderbuffer.c:1237)

## Notes and Other Information
- Handles the transition of a transaction from top-level to subtransaction status gracefully
- Maintains bidirectional references between parent and child transactions
- Critical for proper handling of savepoints and nested transactions in logical replication
- The function includes safeguards to prevent duplicate assignments of the same subtransaction
- Snapshot inheritance ensures that visibility information is properly maintained in the transaction hierarchy
- LSN ordering validation ensures the integrity of the reorder buffer's internal structure