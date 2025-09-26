# ReorderBufferGetInvalidations

## Location
[src/backend/replication/logical/reorderbuffer.c:5478-5492](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/reorderbuffer.c#L5478-L5492)

## Overview
ReorderBufferGetInvalidations retrieves the count and linked list of invalidation messages associated with a specified transaction.

## Definition

```c
uint32
ReorderBufferGetInvalidations(ReorderBuffer *rb, TransactionId xid,
							  SharedInvalidationMessage **msgs)
```
## Detailed Description
ReorderBufferGetInvalidations is a utility function that provides access to invalidation messages stored within a transaction in the reorder buffer. Invalidation messages are critical for maintaining cache coherency in PostgreSQL's shared cache systems during logical replication.

When transactions modify system catalogs or perform operations that affect cached metadata (such as relation definitions, function definitions, or type information), invalidation messages are generated to notify other processes that their cached copies of this information may be stale. During logical decoding, these invalidation messages must be properly handled to ensure that the decoding process sees a consistent view of the database schema.

The function performs a simple lookup operation:
1. Locates the transaction in the reorder buffer using ReorderBufferTXNByXid
2. Returns 0 if the transaction is not found
3. If found, sets the output parameter to point to the transaction's invalidation message list and returns the count

## Parameters / Member Variables
- : The reorder buffer containing transaction data
- : Transaction ID for which to retrieve invalidation messages
- : Output parameter set to point to the linked list of SharedInvalidationMessage structures

## Dependencies
- Functions called/Symbols referenced:
  - [ReorderBufferTXNByXid](ReorderBufferTXNByXid.md)
  - [ReorderBuffer](ReorderBuffer.md)
  - [ReorderBufferTXN](ReorderBufferTXN.md)
  - [SharedInvalidationMessage](../S/SharedInvalidationMessage.md)
- Called from (representative examples):
  - [SnapBuildDistributeSnapshotAndInval](../S/SnapBuildDistributeSnapshotAndInval.md)

## Notes and Other Information
- Returns 0 if the specified transaction is not found in the reorder buffer
- The function provides read-only access to invalidation messages - it does not modify or consume them
- Invalidation messages are essential for maintaining consistency during logical decoding when schema changes occur
- The returned message list should not be modified by the caller
- This function is typically used in conjunction with snapshot building and distribution mechanisms
- The function uses the 'false' parameter for ReorderBufferTXNByXid, indicating it should not create the transaction if it doesn't exist