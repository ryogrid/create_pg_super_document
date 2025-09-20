# ReorderBufferToastAppendChunk

## Location
[src/backend/replication/logical/reorderbuffer.c:4838-4920](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/reorderbuffer.c#L4838-L4920)

## Overview
Processes and stores individual TOAST chunks during logical replication, maintaining proper sequencing and accumulating data needed for eventual reconstruction of large column values.

## Definition

```c
structed = NULL;
```
## Detailed Description
This function handles the processing of individual TOAST chunks as they are encountered during logical replication. When PostgreSQL stores large column values using TOAST, it breaks them into smaller chunks with sequential numbering. This function extracts the chunk ID, sequence number, and data from the TOAST table tuple, then stores it in the transaction's toast hash table for later reconstruction. It performs validation to ensure chunks arrive in the correct sequence (starting from 0 and incrementing by 1), calculates the chunk size accounting for different varlena formats, and maintains metadata about the total size and number of chunks seen. The chunks are stored in a doubly-linked list within the hash entry, preserving their order for reconstruction.

## Parameters / Member Variables
- : Pointer to the ReorderBuffer containing the memory context and configuration
- : Pointer to the ReorderBufferTXN that will track the TOAST chunks
- : The TOAST relation containing the chunk data
- : The ReorderBufferChange containing the new tuple with chunk data

## Dependencies
- Functions called/Symbols referenced:
  - [ReorderBufferToastInitHash](ReorderBufferToastInitHash.md) (initializes toast hash table if needed)
  - [IsToastRelation](../I/IsToastRelation.md) (validates the relation is a TOAST table)
  - [fastgetattr](../f/fastgetattr.md) (extracts chunk_id, chunk_seq, and chunk_data from tuple)
  - [hash_search](../h/hash_search.md) (finds or creates hash entry for chunk_id)
  - [DatumGetObjectId](../D/DatumGetObjectId.md), DatumGetInt32 (converts tuple attributes to proper types)
  - VARATT_IS_EXTENDED, VARATT_IS_SHORT (checks varlena format)
  - VARSIZE, VARSIZE_SHORT (gets size of varlena data)
  - [dlist_init](../d/dlist_init.md), dlist_push_tail (manages linked list of chunks)
- Called from (representative examples):
  - [ReorderBufferProcessTXN](ReorderBufferProcessTXN.md) (during transaction processing)

## Notes and Other Information
- Validates that chunks arrive in strictly sequential order (0, 1, 2, ...)
- Handles different varlena storage formats (normal and short)
- Automatically initializes the toast hash table on first use
- Accumulates total size information needed for efficient reconstruction
- Maintains chunks in insertion order using doubly-linked lists
- Performs extensive error checking for malformed TOAST data
- Critical for logical replication's ability to handle large column values
- The function is static, used only within the reorder buffer implementation