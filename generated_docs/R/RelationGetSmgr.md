# RelationGetSmgr

## Location
[src/include/utils/rel.h:567-581](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/rel.h#L567-L581)

## Overview
Returns the storage manager (smgr) file handle for a relation, opening it if needed. This function provides controlled access to the relation's storage manager handle.

## Definition

```c
static inline SMgrRelation
RelationGetSmgr(Relation rel)
```
## Detailed Description
RelationGetSmgr is an inline function that provides safe access to a relation's storage manager handle (rd_smgr). The function implements lazy initialization - if the smgr handle is NULL, it opens the storage manager for the relation using smgropen() and pins it with smgrpin() to prevent it from being closed prematurely. This design ensures that very little code needs to directly access the rd_smgr field, providing better encapsulation and consistency.

The function uses the unlikely() compiler hint to optimize for the common case where the smgr handle is already initialized, making the NULL check branch prediction more efficient.

## Parameters / Member Variables
- : The relation descriptor for which to get the storage manager handle

## Dependencies
- Functions called/Symbols referenced:
  - [smgropen](../s/smgropen.md): Opens a storage manager relation using the relation's locator and backend ID
  - [smgrpin](../s/smgrpin.md): Pins the storage manager relation to prevent premature closure
- Called from (representative examples):
  - [gistBuildCallback](../g/gistBuildCallback.md): GiST index building
  - [_hash_alloc_buckets](../h/_hash_alloc_buckets.md): Hash index bucket allocation
  - [heapam_relation_copy_data](../h/heapam_relation_copy_data.md): Heap access method data copying
  - [visibilitymap_prepare_truncate](../v/visibilitymap_prepare_truncate.md): Visibility map truncation preparation
  - [table_block_relation_size](../t/table_block_relation_size.md): Table block size calculation
  - [RelationTruncate](RelationTruncate.md): Relation truncation operations
  - [PrefetchBuffer](../P/PrefetchBuffer.md): Buffer prefetching operations
  - [ReadBufferExtended](ReadBufferExtended.md): Extended buffer reading
  - [RelationGetNumberOfBlocksInFork](RelationGetNumberOfBlocksInFork.md): Block count retrieval

## Notes and Other Information
- This is an inline function defined in rel.h for performance reasons
- Very little code should directly access rel->rd_smgr; this function should be used instead
- The function implements lazy initialization to avoid unnecessary smgr opens
- Uses compiler hints (unlikely) for branch prediction optimization
- The returned SMgrRelation handle is pinned to prevent premature closure
- Essential for all storage-level operations on relations including I/O, truncation, and size queries

## Simplified Source

```c
static inline SMgrRelation
RelationGetSmgr(Relation rel)
{
    // Initialize storage manager if not already opened
    if (rel->rd_smgr == NULL) {
        rel->rd_smgr = smgropen(rel->rd_locator, rel->rd_backend);
        smgrpin(rel->rd_smgr);  // Pin to prevent premature closure
    }
    return rel->rd_smgr;
}
```