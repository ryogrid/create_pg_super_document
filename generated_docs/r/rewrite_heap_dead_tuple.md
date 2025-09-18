# rewrite_heap_dead_tuple

## Location
src/backend/access/heap/rewriteheap.c: 543 - 592

## Overview
Processes a dead tuple during heap rewrite by removing any unresolved references to it and freeing associated resources since dead tuples are not copied to the new table.

## Definition


## Detailed Description
The `rewrite_heap_dead_tuple` function handles dead tuples encountered during a heap rewrite operation. Since dead tuples are not copied to the new table, this function's primary purpose is to clean up any unresolved tuple references that may have been waiting for this tuple. This can happen when an earlier tuple in an update chain points to a tuple that is now determined to be dead.

The function performs garbage collection by checking if there are any unresolved tuple entries that reference this dead tuple. If found, it removes these entries from the unresolved tuples hash table and frees the associated memory. This optimization helps prevent memory buildup and reduces the work needed at the end of the rewrite operation.

## Parameters / Member Variables
- `state`: The RewriteState structure containing rewrite context and hash tables for tracking unresolved tuples
- `old_tuple`: The HeapTuple that has been determined to be dead and will not be copied to the new heap

## Dependencies
- Functions called/Symbols referenced:
  - HeapTupleHeaderGetXmin
  - [hash_search](../h/hash_search.md)
  - [heap_freetuple](../h/heap_freetuple.md)
- Called from (representative examples):
  - [heapam_relation_copy_for_cluster](../h/heapam_relation_copy_for_cluster.md)

## Notes and Other Information
- Returns true if an unresolved tuple entry was removed, false otherwise
- Dead tuples are not copied to the new table but their processing helps with resource cleanup
- Handles cases where xmin > xmax, which can occur in valid scenarios but makes tuple liveness detection complex
- Does not attempt to detect dead-followed-by-recently-dead scenarios in the forward direction
- May leave some unmatched entries in UnresolvedTups hash table, which is acceptable as VACUUM operations can remove dead tuples from chains
- Helps optimize memory usage by early cleanup of references to definitely dead tuples
- Part of the tuple chain resolution mechanism that handles complex update chain scenarios during rewrites