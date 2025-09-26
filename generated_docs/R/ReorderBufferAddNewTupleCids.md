# ReorderBufferAddNewTupleCids

## Location
[src/backend/replication/logical/reorderbuffer.c:3331-3357](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/reorderbuffer.c#L3331-L3357)

## Overview
Adds new tuple Command ID (CID) mappings to the reorder buffer, associating tuple identifiers with their command IDs for transaction processing.

## Definition
void ReorderBufferAddNewTupleCids(ReorderBuffer *rb, TransactionId xid, XLogRecPtr lsn, RelFileLocator locator, ItemPointerData tid, CommandId cmin, CommandId cmax, CommandId combocid)

## Detailed Description
This function records mappings between tuple identifiers (relfilelocator, tid) and their associated command IDs (cmin, cmax) in the reorder buffer. These mappings are essential for logical replication to understand the visibility of tuples within transactions. The function creates a REORDER_BUFFER_CHANGE_INTERNAL_TUPLECID change entry and adds it to the transaction's tuplecids list. Notably, this change type is not included in memory accounting because CIDs are kept in a separate list and are not evicted when reaching memory limits.

## Parameters / Member Variables
- : The reorder buffer instance to add the tuple CID mapping to
- : Transaction ID that owns this tuple CID mapping
- : Log Sequence Number where this mapping was recorded
- : RelFileLocator identifying the relation containing the tuple
- : ItemPointerData (tuple identifier) pointing to the specific tuple
- : Command ID when the tuple was inserted/created
- : Command ID when the tuple was deleted/updated (if applicable)
- : Combined command ID for complex cases

## Dependencies
- Functions called/Symbols referenced:
  - [ReorderBufferGetChange](ReorderBufferGetChange.md)
  - [ReorderBufferTXNByXid](ReorderBufferTXNByXid.md)
  - [dlist_push_tail](../d/dlist_push_tail.md)
  - REORDER_BUFFER_CHANGE_INTERNAL_TUPLECID
- Called from (representative examples):
  - [SnapBuildProcessNewCid](../S/SnapBuildProcessNewCid.md)

## Notes and Other Information
- CID mappings are stored separately from regular changes and are not subject to memory eviction
- The function specifically handles internal tuple CID tracking for logical replication
- The change is added to the transaction's tuplecids list using dlist_push_tail
- Transaction's ntuplecids counter is incremented to track the number of CID mappings