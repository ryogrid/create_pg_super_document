# ReorderBufferToastReset

## Location
src/backend/replication/logical/reorderbuffer.c: 5112 - 5174

## Overview
Frees all resources allocated for TOAST reconstruction within a PostgreSQL logical replication transaction, cleaning up both hash table entries and associated chunk data.

## Definition


## Detailed Description
This static function performs comprehensive cleanup of TOAST (The Oversized-Attribute Storage Technique) reconstruction resources for a given transaction in PostgreSQL's logical replication system. TOAST is used to handle large column values that exceed the page size limit by breaking them into smaller chunks stored separately.

The function operates by:
1. Checking if the transaction has an active toast hash table
2. If present, sequentially iterating through all entries in the hash table
3. For each entry, freeing any reconstructed TOAST data and cleaning up associated chunk lists
4. Deallocating individual ReorderBufferChange objects from the chunk lists
5. Finally destroying the entire hash table and setting the pointer to NULL

This cleanup is essential to prevent memory leaks in long-running logical replication sessions where many TOAST values may be processed.

## Parameters / Member Variables
- : Pointer to the main ReorderBuffer structure managing the logical replication session
- : Pointer to the ReorderBufferTXN structure representing the transaction whose TOAST data should be cleaned up

## Dependencies
- Functions called/Symbols referenced:
  - [hash_seq_init](../h/hash_seq_init.md)
  - [hash_seq_search](../h/hash_seq_search.md)
  - [pfree](../p/pfree.md)
  - dlist_foreach_modify
  - dlist_container
  - [dlist_delete](../d/dlist_delete.md)
  - [ReorderBufferReturnChange](ReorderBufferReturnChange.md)
  - [hash_destroy](../h/hash_destroy.md)
- Called from (representative examples):
  - [ReorderBufferReturnTXN](ReorderBufferReturnTXN.md)
  - [ReorderBufferResetTXN](ReorderBufferResetTXN.md)
  - [ReorderBufferProcessTXN](ReorderBufferProcessTXN.md)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the reorderbuffer.c file
- The function safely handles the case where txn->toast_hash is NULL by returning early
- Uses PostgreSQL's doubly-linked list (dlist) and hash table utilities for efficient memory management
- Critical for preventing memory leaks in logical replication scenarios involving large column values
- The function ensures complete cleanup by both freeing reconstructed data and returning ReorderBufferChange objects to the buffer pool