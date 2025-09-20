# begin_heap_rewrite

## Location
[src/backend/access/heap/rewriteheap.c:234-296](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/rewriteheap.c#L234-L296)

## Overview
Initializes a table rewrite operation by setting up necessary data structures and memory contexts for rewriting tuples from an old heap relation to a new heap relation.

## Definition

```c
struct itself plus all subsidiary data.
	 */
	rw_cxt = AllocSetContextCreate(CurrentMemoryContext,
								   "Table rewrite",
								   ALLOCSET_DEFAULT_SIZES);
```
## Detailed Description
The  function starts a heap rewrite operation, which is typically used during operations like CLUSTER, VACUUM FULL, or ALTER TABLE that require reorganizing table data. It creates a dedicated memory context for the rewrite operation and initializes a RewriteState structure that tracks the progress and metadata needed throughout the rewrite process.

The function sets up hash tables to track tuple update chains and maintains mappings between old and new tuple identifiers (TIDs). It also initializes bulk write operations on the new relation for efficient data insertion. The function ensures proper cleanup by creating all subsidiary data within a separate memory context.

## Parameters / Member Variables
- : The source relation from which tuples will be read during the rewrite operation
- : The destination relation where rewritten tuples will be inserted (must be locked but needn't be empty)
- : Transaction ID used to determine which tuples are considered dead and can be removed
- : Transaction ID threshold before which tuples will be frozen to prevent wraparound issues
- : MultiXact ID threshold before which multixacts will be removed during the rewrite

## Dependencies
- Functions called/Symbols referenced:
  - AllocSetContextCreate
  - RelationGetNumberOfBlocks
  - [smgr_bulk_start_rel](../s/smgr_bulk_start_rel.md)
  - [hash_create](../h/hash_create.md)
  - [logical_begin_heap_rewrite](../l/logical_begin_heap_rewrite.md)
- Called from (representative examples):
  - [heapam_relation_copy_for_cluster](../h/heapam_relation_copy_for_cluster.md)

## Notes and Other Information
- Creates a dedicated memory context "Table rewrite" for easy cleanup of all rewrite-related data
- Initializes two hash tables: one for unresolved tuple chains and another for old-to-new TID mappings
- The new heap relation doesn't need to be empty, only locked, allowing for incremental rewrites
- Uses bulk write operations for efficient insertion into the new relation
- Integrates with logical replication by calling logical_begin_heap_rewrite for change tracking