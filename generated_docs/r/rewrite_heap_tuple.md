# rewrite_heap_tuple

## Location
[src/backend/access/heap/rewriteheap.c:341-542](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/rewriteheap.c#L341-L542)

## Overview
Rewrites and inserts a tuple into the new heap during a table rewrite operation, handling tuple visibility information, update chains, and cross-references between old and new tuple locations.

## Definition

```c
void
rewrite_heap_tuple(RewriteState state,
				   HeapTuple old_tuple, HeapTuple new_tuple)
```
## Detailed Description
The `rewrite_heap_tuple` function is responsible for processing individual tuples during a heap rewrite operation. It copies visibility information from the original tuple to the new tuple, applies freezing to old transactions, and manages complex update chain relationships. The function handles the intricate task of maintaining tuple update chains across the rewrite by using hash tables to track unresolved tuple references and old-to-new TID mappings.

The function manages update chains by checking if a tuple is part of an update chain and either resolves existing forward references or creates new mapping entries for future resolution. It uses a loop to process cascading tuple chain resolutions, where resolving one tuple may trigger the resolution of previously unresolved tuples that were waiting for this tuple's location.

## Parameters / Member Variables
- `state`: The RewriteState structure containing rewrite context, hash tables, and configuration parameters
- `old_tuple`: The original HeapTuple from the old heap relation that serves as the source
- `new_tuple`: The new HeapTuple to be written to the new heap relation (modified by this function)

## Dependencies
- Functions called/Symbols referenced:
  - [heap_freeze_tuple](../h/heap_freeze_tuple.md)
  - [ItemPointerSetInvalid](../I/ItemPointerSetInvalid.md)
  - [HeapTupleHeaderIsOnlyLocked](../H/HeapTupleHeaderIsOnlyLocked.md)
  - HeapTupleHeaderIndicatesMovedPartitions
  - [ItemPointerEquals](../I/ItemPointerEquals.md)
  - HeapTupleHeaderGetUpdateXid
  - [hash_search](../h/hash_search.md)
  - [heap_copytuple](../h/heap_copytuple.md)
  - [raw_heap_insert](raw_heap_insert.md)
  - [logical_rewrite_heap_tuple](../l/logical_rewrite_heap_tuple.md)
  - [TransactionIdPrecedes](../T/TransactionIdPrecedes.md)
  - HeapTupleHeaderGetXmin
  - [heap_freetuple](../h/heap_freetuple.md)
- Called from (representative examples):
  - [reform_and_rewrite_tuple](reform_and_rewrite_tuple.md)

## Notes and Other Information
- Copies transaction visibility information from old to new tuple while clearing HOT status bits
- Applies tuple freezing to prevent transaction wraparound issues in the new heap
- Manages complex update chain resolution using two hash tables: rs_unresolved_tups and rs_old_new_tid_map
- Handles cascading chain resolution through a loop that processes newly resolved tuples
- Integrates with logical replication by calling logical_rewrite_heap_tuple for change tracking
- Uses temporary memory context switching to ensure proper memory management
- May defer tuple insertion if it's part of an unresolved update chain
- Properly handles tuple freeing to prevent memory leaks in complex chain scenarios