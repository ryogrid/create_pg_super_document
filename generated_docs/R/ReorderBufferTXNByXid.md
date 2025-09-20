# ReorderBufferTXNByXid

## Location
[src/backend/replication/logical/reorderbuffer.c:649-736](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/reorderbuffer.c#L649-L736)

## Overview
Returns a ReorderBufferTXN from the reorder buffer by transaction ID (Xid), with optional creation and caching for efficient lookups during logical replication decoding.

## Definition

```c
static ReorderBufferTXN *
ReorderBufferTXNByXid(ReorderBuffer *rb, TransactionId xid, bool create,
					  bool *is_new, XLogRecPtr lsn, bool create_as_top)
```
## Detailed Description
This function serves as the primary interface for retrieving transaction objects from the reorder buffer during logical replication. It implements a two-level lookup strategy: first checking a single-entry cache for the most recently accessed transaction, then falling back to a hash table lookup. If the create flag is set and the transaction doesn't exist, it allocates a new ReorderBufferTXN and initializes it with the provided LSN. The function also maintains the toplevel_by_lsn ordering when creating top-level transactions and updates the lookup cache for subsequent accesses.

## Parameters / Member Variables
- : The ReorderBuffer containing the transaction hash table and cache
- : The transaction ID to look up or create
- : Whether to create a new transaction if it doesn't exist
- : Output parameter indicating if a new transaction was created (can be NULL)
- : The LSN to use when creating a new transaction (must be valid if create is true)
- : Whether to add the new transaction to the top-level transaction list ordered by LSN

## Dependencies
- Functions called/Symbols referenced:
  - [hash_search](../h/hash_search.md) (hash table lookup/insertion)
  - [ReorderBufferGetTXN](ReorderBufferGetTXN.md) (allocates new transaction object)
  - [dlist_push_tail](../d/dlist_push_tail.md) (adds to top-level transaction list)
  - [AssertTXNLsnOrder](../A/AssertTXNLsnOrder.md) (debug assertion for LSN ordering)
- Called from (representative examples):
  - [ReorderBufferQueueChange](ReorderBufferQueueChange.md) (queuing transaction changes)
  - [ReorderBufferQueueMessage](ReorderBufferQueueMessage.md) (queuing transaction messages)
  - [ReorderBufferCommit](ReorderBufferCommit.md) (committing transactions)
  - ReorderBufferAssignChild (assigning subtransactions)

## Notes and Other Information
- Uses a single-entry cache (by_txn_last_xid/by_txn_last_txn) to optimize repeated lookups of the same transaction
- The cache can store NULL values to remember that a transaction doesn't exist, avoiding repeated hash table lookups
- When creating new transactions, they are initialized with restart_decoding_lsn from the reorder buffer
- Top-level transactions are maintained in LSN order in the toplevel_by_lsn list for proper commit ordering
- The function is static and only used within the reorderbuffer.c module