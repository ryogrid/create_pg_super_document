# ReorderBufferBuildTupleCidHash

## Location
[src/backend/replication/logical/reorderbuffer.c:1778-1850](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/reorderbuffer.c#L1778-L1850)

## Overview
Builds a hash table mapping (relfilelocator, ctid) pairs to command ID information (cmin, cmax, combocid) for use by HeapTupleSatisfiesHistoricMVCC during catalog snapshot access.

## Definition

```c
static void
ReorderBufferBuildTupleCidHash(ReorderBuffer *rb, ReorderBufferTXN *txn)
```
## Detailed Description
This function constructs a specialized hash table that enables efficient lookup of command ID information for specific tuples during logical decoding. The hash table is essential for proper MVCC (Multi-Version Concurrency Control) visibility checking when accessing catalog tables during logical replication.

The function operates as follows:

1. **Validation**: First checks if the transaction has catalog changes and contains tuplecid data. If either condition is false, the function returns early without creating the hash table.

2. **Hash table creation**: Creates a hash table with the exact size needed to store all tuplecids, using:
   - Key: ReorderBufferTupleCidKey (relfilelocator + ctid)  
   - Value: ReorderBufferTupleCidEnt (cmin, cmax, combocid)
   - Context: Uses the reorder buffer's memory context for allocation

3. **Data population**: Iterates through all tuplecid changes in the transaction and populates the hash table:
   - Extracts relfilelocator and ctid from each change to form the key
   - For new entries: stores cmin, cmax, and combocid values
   - For existing entries: validates cmin consistency and updates cmax if needed

4. **Validation logic**: Implements strict validation rules:
   - cmin must be consistent across multiple references to the same tuple
   - cmax can only grow and cannot become invalid once set
   - These rules ensure MVCC correctness during catalog access

The resulting hash table provides O(1) lookup time for tuple command ID information, which is crucial for performance when HeapTupleSatisfiesHistoricMVCC needs to determine tuple visibility.

## Parameters / Member Variables
- `*rb`: Pointer to the main ReorderBuffer structure containing the memory context for hash table allocation
- `*txn`: Pointer to the ReorderBufferTXN structure containing the tuplecids to be indexed and where the resulting hash table will be stored
## Dependencies
- Functions called/Symbols referenced:
  - rbtxn_has_catalog_changes
  - [dlist_is_empty](../d/dlist_is_empty.md)
  - [hash_create](../h/hash_create.md)
  - dlist_foreach
  - dlist_container
  - memset
  - [ItemPointerCopy](../I/ItemPointerCopy.md)
  - [hash_search](../h/hash_search.md)
- Called from (representative examples):
  - [ReorderBufferProcessTXN](ReorderBufferProcessTXN.md)

## Notes and Other Information
- This is a static function, accessible only within reorderbuffer.c
- The hash table is created with exact sizing (txn->ntuplecids) for memory efficiency
- Memory padding is carefully handled when creating hash keys to ensure proper key comparison
- The function only processes REORDER_BUFFER_CHANGE_INTERNAL_TUPLECID type changes
- [Hash](../H/Hash.md) table uses HASH_ELEM, HASH_BLOBS, and HASH_CONTEXT flags for proper setup
- The resulting hash table is stored in txn->tuplecid_hash for later use
- Essential component of PostgreSQL's logical replication system for maintaining MVCC consistency
- The hash table lifetime is tied to the transaction and is cleaned up when the transaction is truncated or cleaned up