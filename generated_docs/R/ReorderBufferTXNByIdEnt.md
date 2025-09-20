# ReorderBufferTXNByIdEnt

## Location
[src/backend/replication/logical/reorderbuffer.c:128-132](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/reorderbuffer.c#L128-L132)

## Overview
ReorderBufferTXNByIdEnt is a hash table entry structure used to map transaction IDs (xid) to their corresponding transaction state objects in PostgreSQL's logical replication reorder buffer system.

## Definition

```c
typedef struct ReorderBufferTXNByIdEnt
{
	TransactionId xid;
	ReorderBufferTXN *txn;
} ReorderBufferTXNByIdEnt;
```
## Detailed Description
This structure serves as an entry in a hash table that provides efficient lookup of transaction state information based on transaction ID. The reorder buffer is a crucial component of PostgreSQL's logical replication system, responsible for collecting and reordering WAL changes before they are decoded and sent to subscribers. This hash table entry allows the system to quickly locate the ReorderBufferTXN structure associated with a specific transaction ID during the logical decoding process.

## Parameters / Member Variables
- `xid`: The transaction ID (TransactionId) that serves as the key for hash table lookups
- `*txn`: Pointer to the ReorderBufferTXN structure containing the complete transaction state and accumulated changes
## Dependencies
- Functions called/Symbols referenced:
  - TransactionId (PostgreSQL transaction ID type)
  - [ReorderBufferTXN](ReorderBufferTXN.md) (transaction state structure)
- Called from (representative examples):
  - [ReorderBufferAllocate](ReorderBufferAllocate.md) (at src/backend/replication/logical/reorderbuffer.c:368)
  - [ReorderBufferTXNByXid](ReorderBufferTXNByXid.md) (at src/backend/replication/logical/reorderbuffer.c:653, 689)

## Notes and Other Information
This structure is specifically designed for use with PostgreSQL's hash table implementation (dynahash.c). The hash table using this entry type enables O(1) average-case lookup time for finding transaction state information during logical replication decoding, which is critical for performance when processing large numbers of concurrent transactions.