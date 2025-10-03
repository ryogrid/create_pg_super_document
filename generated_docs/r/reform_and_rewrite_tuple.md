# reform_and_rewrite_tuple

## Location
[src/backend/access/heap/heapam_handler.c:2513-2542](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam_handler.c#L2513-L2542)

## Overview
reform_and_rewrite_tuple reconstructs and rewrites a tuple during table rewrite operations, handling dropped columns and ensuring compatibility with the new table structure.

## Definition

```c
static void
reform_and_rewrite_tuple(HeapTuple tuple,
						 Relation OldHeap, Relation NewHeap,
						 Datum *values, bool *isnull, RewriteState rwstate)
```
## Detailed Description
This function is a critical helper function used during table rewrite operations (such as ALTER TABLE). It reconstructs tuples from the old table format to the new table format, ensuring proper handling of structural changes. The function cannot simply copy tuples as-is for several important reasons:

1. **Dropped column handling**: It squeezes out values of any dropped columns to save space and prevent corner-case failures (e.g., when the new table lacks a TOAST table and cannot store large values from dropped columns).

2. **Compatibility enforcement**: The original tuple might not be legal for the new table structure, particularly after operations like ALTER TABLE SET WITHOUT OIDS.

The function decomposes the original tuple into its component Datums, nullifies any dropped columns in the new schema, reconstructs the tuple according to the new table descriptor, and then delegates the actual rewriting to the heap rewrite module.

## Parameters / Member Variables
- : The original HeapTuple from the old table that needs to be rewritten
- : Relation pointer to the source table with the original structure
- : Relation pointer to the destination table with the new structure  
- : Array of Datum values to store the deformed tuple components
- : Array of boolean flags indicating which values are NULL
- : RewriteState context that manages the overall rewrite operation

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetDescr (via OldHeap and NewHeap)
  - [heap_deform_tuple](../h/heap_deform_tuple.md)
  - TupleDescAttr
  - [heap_form_tuple](../h/heap_form_tuple.md)
  - [rewrite_heap_tuple](rewrite_heap_tuple.md)
  - [heap_freetuple](../h/heap_freetuple.md)
  - [RewriteState](../R/RewriteState.md)
- Called from (representative examples):
  - [heapam_relation_copy_for_cluster](../h/heapam_relation_copy_for_cluster.md)

## Notes and Other Information
- This is a static function internal to heapam_handler.c, specifically designed for table clustering and rewrite operations
- The function ensures that dropped columns are properly nullified in the new tuple structure
- Memory management is handled carefully - the reconstructed tuple is freed after being passed to the rewrite module
- This function is essential for maintaining data integrity during table structure modifications while optimizing storage space

## Simplified Source

```c
static void
reform_and_rewrite_tuple(HeapTuple tuple,
                         Relation OldHeap, Relation NewHeap,
                         Datum *values, bool *isnull, RewriteState rwstate)
{
    TupleDesc oldTupDesc = RelationGetDescr(OldHeap);
    TupleDesc newTupDesc = RelationGetDescr(NewHeap);
    HeapTuple copiedTuple;

    // Extract all attribute values from the old tuple
    heap_deform_tuple(tuple, oldTupDesc, values, isnull);

    // Null out any dropped columns in the new table structure
    for (int i = 0; i < newTupDesc->natts; i++) {
        if (TupleDescAttr(newTupDesc, i)->attisdropped)
            isnull[i] = true;
    }

    // Reconstruct tuple according to new table structure
    copiedTuple = heap_form_tuple(newTupDesc, values, isnull);

    // Hand off to heap rewrite module for actual writing
    rewrite_heap_tuple(rwstate, tuple, copiedTuple);

    // Clean up the temporary tuple
    heap_freetuple(copiedTuple);
}
```