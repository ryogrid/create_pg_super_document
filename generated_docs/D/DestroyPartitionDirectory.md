# DestroyPartitionDirectory

## Location
[src/backend/partitioning/partdesc.c:484-500](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/partitioning/partdesc.c#L484-L500)

## Overview
Destroys a partition directory by releasing all relation reference counts held by its cached entries.

## Definition


## Detailed Description
DestroyPartitionDirectory properly cleans up a partition directory by iterating through all cached entries and releasing the reference counts on relations that were incremented during PartitionDirectoryLookup operations. This is essential for proper memory management and preventing relation leaks.

The function uses PostgreSQL's hash table sequential scan interface to iterate through all entries in the partition directory's hash table. For each entry found, it decrements the reference count on the associated relation, which was previously incremented when the entry was created.

Note that this function only handles the reference count cleanup; the actual memory deallocation of the partition directory and its hash table is handled by the memory context system when the directory's memory context is destroyed.

## Parameters / Member Variables
- : The partition directory to destroy

## Dependencies
- Functions called/Symbols referenced:
  - HASH_SEQ_STATUS (hash sequential scan status structure)
  - [hash_seq_init](../h/hash_seq_init.md)
  - [hash_seq_search](../h/hash_seq_search.md)  
  - [RelationDecrementReferenceCount](../R/RelationDecrementReferenceCount.md)
  - [PartitionDirectoryEntry](../P/PartitionDirectoryEntry.md) (hash table entry type)

- Called from:
  - [FreeExecutorState](../F/FreeExecutorState.md)
  - [standard_planner](../s/standard_planner.md)

## Notes and Other Information
- Must be called to properly clean up partition directories and avoid relation reference leaks
- Only handles reference count cleanup; memory deallocation is handled by memory context destruction
- Uses hash table sequential scan to iterate through all cached entries
- Essential for maintaining proper relation reference counting in PostgreSQL's resource management
- Typically called during cleanup phases of query execution or planning
- The function does not explicitly destroy the hash table or memory context - this is handled externally