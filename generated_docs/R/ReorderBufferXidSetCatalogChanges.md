# ReorderBufferXidSetCatalogChanges

## Location
[src/backend/replication/logical/reorderbuffer.c:3530-3567](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/reorderbuffer.c#L3530-L3567)

## Overview
Marks a transaction as containing catalog changes and maintains the catalog changes transaction list for logical replication processing.

## Definition
void ReorderBufferXidSetCatalogChanges(ReorderBuffer *rb, TransactionId xid, XLogRecPtr lsn)

## Detailed Description
This function marks a transaction as having made catalog changes, which is crucial for logical replication. When a transaction modifies system catalogs, it needs special handling during replication replay. The function sets the RBTXN_HAS_CATALOG_CHANGES flag on the transaction and adds it to the reorder buffer's catalog changes transaction list. Additionally, if the transaction is a subtransaction, it also marks the top-level transaction as having catalog changes to simplify checks during tuple CID hash table construction.

## Parameters / Member Variables
- rb: Pointer to the ReorderBuffer structure managing transaction state
- xid: Transaction ID of the transaction to mark as having catalog changes
- lsn: Log Sequence Number where the catalog change was detected

## Dependencies
- Functions called/Symbols referenced:
  - [ReorderBufferTXNByXid](ReorderBufferTXNByXid.md)
  - rbtxn_has_catalog_changes
  - rbtxn_is_subtxn
  - rbtxn_get_toptxn
  - [dclist_push_tail](../d/dclist_push_tail.md)
  - RBTXN_HAS_CATALOG_CHANGES
- Called from (representative examples):
  - [xact_decode](../x/xact_decode.md)
  - [heap_decode](../h/heap_decode.md)
  - [SnapBuildProcessNewCid](../S/SnapBuildProcessNewCid.md)

## Notes and Other Information
- Essential for logical replication to properly handle transactions that modify system catalogs
- Maintains a doubly-linked list of transactions with catalog changes for efficient processing
- When subtransactions have catalog changes, the top-level transaction is also marked to optimize ReorderBufferBuildTupleCidHash operations
- The catalog changes flag affects how transactions are processed during logical decoding and replication