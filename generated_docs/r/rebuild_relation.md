# rebuild_relation

## Location
[src/backend/commands/cluster.c:633-687](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/cluster.c#L633-L687)

## Overview
Rebuilds an existing relation in either index order (clustering) or physical order by creating a new heap, copying data in the desired order, and swapping the files.

## Definition


## Detailed Description
The  function is the core implementation for PostgreSQL's table clustering and rewriting operations. It performs a complete reconstruction of a table by:

1. Creating a new temporary heap table with the same structure as the original
2. Copying all data from the original table to the new table, either in index order (for clustering) or physical order (for general rewriting)
3. Swapping the physical files between the original and new tables
4. Rebuilding all indexes and cleaning up the temporary table

This function is essential for the CLUSTER command and other table reorganization operations. It ensures data consistency through exclusive locking and handles system catalogs appropriately.

## Parameters / Member Variables
- : The relation to be rebuilt - must be opened and exclusive-locked by the caller
- : OID of the index to cluster by, or InvalidOid to rewrite in physical order
- : Boolean flag to control verbose output during the operation

## Dependencies
- Functions called/Symbols referenced:
  - [mark_index_clustered](../m/mark_index_clustered.md): Marks the specified index as clustered
  - [IsSystemRelation](../I/IsSystemRelation.md): Checks if the relation is a system catalog
  - [make_new_heap](../m/make_new_heap.md): Creates the new temporary heap table
  - [copy_table_data](../c/copy_table_data.md): Copies data from old to new heap in desired order
  - [finish_heap_swap](../f/finish_heap_swap.md): Swaps files and completes the rebuilding process
  - table_close: Closes the relation handle
  - RelationGetRelid: Gets the OID of the relation
- Called from (representative examples):
  - [cluster_rel](../c/cluster_rel.md): Main clustering function
  - RelToCluster: Part of cluster processing workflow

## Notes and Other Information
- The function closes the OldHeap relation at the appropriate time; callers should not close it themselves
- Maintains exclusive lock on the table throughout the operation until transaction commit
- Handles both regular tables and system catalogs appropriately
- Preserves table persistence characteristics (permanent, temporary, etc.)
- Critical for maintaining data integrity during table reorganization operations
- The operation is atomic from the user's perspective due to the file swapping mechanism