# ReorderBufferToastInitHash

## Location
[src/backend/replication/logical/reorderbuffer.c:4818-4837](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/reorderbuffer.c#L4818-L4837)

## Overview
Initializes a hash table for TOAST (The Oversized-Attribute Storage Technique) chunk reassembly within a reorder buffer transaction to reconstruct large column values during logical replication.

## Definition

```c
static void
ReorderBufferToastInitHash(ReorderBuffer *rb, ReorderBufferTXN *txn)
```
## Detailed Description
This function creates a hash table specifically designed to handle TOAST data reassembly during logical replication. When PostgreSQL stores large column values (typically over 2KB), it uses TOAST to break them into smaller chunks stored in separate TOAST tables. During logical replication, these chunks need to be reassembled to reconstruct the original large values. The hash table uses OIDs as keys to track different TOAST entities and stores ReorderBufferToastEnt structures that maintain the state of chunk reassembly for each large value. The hash table is created in the reorder buffer's memory context to ensure proper memory management.

## Parameters / Member Variables
- `*rb`: Pointer to the ReorderBuffer containing the memory context for hash table allocation
- `*txn`: Pointer to the ReorderBufferTXN that will own the toast_hash for tracking TOAST reassembly
## Dependencies
- Functions called/Symbols referenced:
  - [hash_create](../h/hash_create.md) (creates the hash table with specified parameters)
  - Assert (ensures toast_hash is NULL before initialization)
  - [HASHCTL](../H/HASHCTL.md) (hash table control structure)
  - [ReorderBufferToastEnt](ReorderBufferToastEnt.md) (hash table entry structure for TOAST data)
  - HASH_ELEM, HASH_BLOBS, HASH_CONTEXT (hash table configuration flags)
- Called from (representative examples):
  - [ReorderBufferToastAppendChunk](ReorderBufferToastAppendChunk.md) (when first TOAST chunk is encountered)

## Notes and Other Information
- The function is static, indicating it's only used within reorderbuffer.c
- [Hash](../H/Hash.md) table is configured with OID keys (sizeof(Oid)) to identify different TOAST entities
- Uses a small initial size (5 buckets) as TOAST usage is typically sparse
- The HASH_BLOBS flag enables efficient handling of binary key data
- HASH_CONTEXT ensures the hash table is allocated in the reorder buffer's memory context
- Critical for reconstructing large column values during logical replication decode operations
- Must only be called when txn->toast_hash is NULL (enforced by Assert)

## Simplified Source

```c
static void
ReorderBufferToastInitHash(ReorderBuffer *rb, ReorderBufferTXN *txn)
{
	// Ensure hash table is not already initialized
	Assert(txn->toast_hash == NULL);

	// Configure hash table for TOAST entries
	HASHCTL hash_ctl;
	hash_ctl.keysize = sizeof(Oid);  // Use OID as key
	hash_ctl.entrysize = sizeof(ReorderBufferToastEnt);
	hash_ctl.hcxt = rb->context;  // Use reorder buffer's memory context

	// Create hash table for tracking TOAST chunks
	txn->toast_hash = hash_create("ReorderBufferToastHash", 5, &hash_ctl,
								  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}
```