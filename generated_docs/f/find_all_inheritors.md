# find_all_inheritors

## Location
[src/backend/catalog/pg_inherits.c:255-354](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_inherits.c#L255-L354)

## Overview
Returns a list of relation OIDs including the given relation plus all relations that inherit from it, directly or indirectly, with optional parent count tracking.

## Definition
```c
List *find_all_inheritors(Oid parentrelId, LOCKMODE lockmode, List **numparents)
```

## Detailed Description
This function performs a breadth-first traversal of the complete inheritance hierarchy rooted at the specified parent relation. It returns all descendants in the inheritance tree, including the root relation itself. The function uses a hash table for efficient duplicate detection during traversal, ensuring that relations appearing in multiple inheritance paths are only included once.

The algorithm maintains a work queue (using the same list as the result) and processes each relation to find its direct children using find_inheritance_children. For each child found, it either adds the child to the result set or increments the parent counter if the child was already discovered through a different inheritance path.

The function excludes detached partitions automatically since it relies on find_inheritance_children, which omits such partitions.

## Parameters / Member Variables
- `parentrelId`: OID of the root relation whose complete inheritance tree should be found
- `lockmode`: Lock mode to acquire on all child relations; use NoLock to skip locking
- `numparents`: Optional output parameter that returns a parallel list containing the number of parents found for each relation in the inheritance tree (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [hash_create](../h/hash_create.md) (with HASH_ELEM, HASH_BLOBS, HASH_CONTEXT)
  - list_make1_oid
  - list_make1_int
  - [find_inheritance_children](find_inheritance_children.md)
  - [hash_search](../h/hash_search.md) (with HASH_ENTER)
  - list_nth_cell
  - lfirst_int
  - lappend_oid
  - lappend_int
  - [list_free](../l/list_free.md)
  - [hash_destroy](../h/hash_destroy.md)
  - [SeenRelsEntry](../S/SeenRelsEntry.md) (hash table entry structure)
- Called from (representative examples):
  - [GetPubPartitionOptionRelations](../G/GetPubPartitionOptionRelations.md) (src/backend/catalog/pg_publication.c:273)
  - [acquire_inherited_sample_rows](../a/acquire_inherited_sample_rows.md) (src/backend/commands/analyze.c:1369)
  - [get_tables_to_cluster_partitioned](../g/get_tables_to_cluster_partitioned.md) (src/backend/commands/cluster.c:1698)
  - [DefineIndex](../D/DefineIndex.md) (src/backend/commands/indexcmds.c:1289)
  - [LockTableRecurse](../L/LockTableRecurse.md) (src/backend/commands/lockcmds.c:122)
  - [RemoveRelations](../R/RemoveRelations.md) (src/backend/commands/tablecmds.c:1608)
  - [ExecuteTruncate](../E/ExecuteTruncate.md) (src/backend/commands/tablecmds.c:1838)
  - [expand_inherited_rtentry](../e/expand_inherited_rtentry.md) (src/backend/optimizer/util/inherit.c:170)

## Notes and Other Information
- Uses a hash table with O(1) lookup to efficiently detect duplicate relations in the inheritance graph
- The first element in the returned list is always the root parentrelId with 0 parents
- When numparents is provided, it returns a parallel list where each element indicates how many direct parents that relation has within the tree
- Multiple inheritance paths to the same child are handled by incrementing the parent count rather than duplicating entries
- The algorithm is designed to handle complex inheritance hierarchies while avoiding infinite loops (though cycles shouldn't exist in PostgreSQL's inheritance graph)
- No provision is made for including concurrently detached partitions, as no current callers require this functionality
- The caller is responsible for locking the root relation before calling this function
- Located in src/backend/catalog/pg_inherits.c:255-354