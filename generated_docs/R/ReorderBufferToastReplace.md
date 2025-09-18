# ReorderBufferToastReplace

## Location
[src/backend/replication/logical/reorderbuffer.c:4921-5111](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/reorderbuffer.c#L4921-L5111)

## Overview
Reconstructs TOAST values from collected chunks and replaces external TOAST pointers in tuple data with in-memory reconstructed values during logical replication processing.

## Definition


## Detailed Description
This function performs the critical task of reconstructing large column values that were stored using PostgreSQL's TOAST mechanism during logical replication. When a transaction is ready for processing, any external TOAST pointers in the tuple data need to be replaced with the actual reconstructed values built from the chunks collected by ReorderBufferToastAppendChunk. The function iterates through all attributes in the tuple, identifies external TOAST pointers, looks up the corresponding chunks in the transaction's toast hash table, reassembles the chunks into the original large value, and replaces the external pointer with an indirect pointer to the reconstructed data. It also carefully manages memory accounting by tracking the size difference between the original change and the modified change with reconstructed TOAST data.

## Parameters / Member Variables
- : Pointer to the ReorderBuffer containing memory context and configuration
- : Pointer to the ReorderBufferTXN containing the toast_hash with collected chunks
- : The base relation (not the TOAST relation) being processed
- : The ReorderBufferChange containing the tuple to be modified

## Dependencies
- Functions called/Symbols referenced:
  - [ReorderBufferChangeSize](ReorderBufferChangeSize.md) (calculates change size for memory accounting)
  - [RelationIdGetRelation](RelationIdGetRelation.md), RelationClose (accesses TOAST relation)
  - [heap_deform_tuple](../h/heap_deform_tuple.md), heap_form_tuple (tuple manipulation)
  - VARATT_IS_EXTERNAL, VARATT_EXTERNAL_GET_POINTER (TOAST pointer analysis)
  - [hash_search](../h/hash_search.md) (finds TOAST entries in hash table)
  - dlist_foreach, dlist_container (iterates through chunk list)
  - SET_VARTAG_EXTERNAL, VARDATA_EXTERNAL (creates indirect pointers)
  - ReorderBufferChangeMemoryUpdate (updates memory accounting)
  - Various TOAST macros for size and compression handling
- Called from (representative examples):
  - [ReorderBufferProcessTXN](ReorderBufferProcessTXN.md) (during transaction commit processing)

## Notes and Other Information
- Only processes changes that have collected TOAST chunks (txn->toast_hash != NULL)
- Handles both compressed and uncompressed TOAST values appropriately  
- Creates indirect pointers to reconstructed data rather than inline storage
- Carefully manages memory accounting to prevent serialization triggers during commit
- Validates chunk data integrity during reconstruction (no external or short chunks)
- Allocates reconstructed data in the reorder buffer's memory context
- Critical for ensuring large column values are available during logical replication output
- The function is static, used only within the reorder buffer implementation
- Must be called after all TOAST chunks for a transaction have been collected