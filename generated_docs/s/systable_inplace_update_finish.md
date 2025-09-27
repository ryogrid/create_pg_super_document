# systable_inplace_update_finish

## Location
[src/backend/access/index/genam.c:873-891](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/index/genam.c#L873-L891)

## Overview
Completes an in-place tuple update operation by performing the actual overwrite of tuple data and properly cleaning up the scan state.

## Definition
void systable_inplace_update_finish(void *state, HeapTuple tuple)

## Detailed Description
This function represents the second and final phase of the in-place update process initiated by systable_inplace_update_begin(). It performs the actual tuple overwrite using the provided modified tuple data and handles proper cleanup of the scan state.

The function operates under strict constraints: the updated tuple cannot change size, which means its header fields and null bitmap remain unchanged. This size restriction is fundamental to the safety of in-place updates, as changing tuple size would require complex page reorganization that would violate the locking assumptions of concurrent readers.

The core update operation is delegated to heap_inplace_update_and_unlock(), which handles the low-level details of copying the new data into the existing tuple and releasing the exclusive locks acquired during the begin phase. After the update is complete, the function properly terminates the scan that was used to locate and lock the tuple.

## Parameters / Member Variables
- `state`: Opaque state pointer returned from systable_inplace_update_begin(), containing the SysScanDesc
- `tuple`: Modified tuple data to write over the existing tuple (must be same size as original)

## Dependencies
- Functions called/Symbols referenced:
  - [heap_inplace_update_and_unlock](../h/heap_inplace_update_and_unlock.md)
  - [systable_endscan](systable_endscan.md)
  - [SysScanDesc](../S/SysScanDesc.md) (type)
  - [BufferHeapTupleTableSlot](../B/BufferHeapTupleTableSlot.md) (type)
- Called from (representative examples):
  - [index_update_stats](../i/index_update_stats.md)
  - [create_toast_table](../c/create_toast_table.md)  
  - [dropdb](../d/dropdb.md)
  - [EventTriggerOnLogin](../E/EventTriggerOnLogin.md)
  - [vac_update_relstats](../v/vac_update_relstats.md)
  - [vac_update_datfrozenxid](../v/vac_update_datfrozenxid.md)

## Notes and Other Information
- Must be paired with a prior successful call to systable_inplace_update_begin()
- The tuple parameter must have the same size as the original tuple found during begin phase
- Tuple header fields and null bitmap cannot be modified due to size constraints
- Automatically handles lock release and scan cleanup
- Should not be called if no actual changes were made to the tuple (use systable_inplace_update_cancel() instead)
- The state parameter becomes invalid after this call and should not be reused
- Part of the three-function in-place update API: begin, finish, and cancel

## Simplified Source

```c
// Simplified version of systable_inplace_update_finish
void systable_inplace_update_finish(void *state, HeapTuple tuple) {
    // Cast the opaque state back to scan descriptor
    SysScanDesc scan = (SysScanDesc) state;

    // Extract components from the scan state
    Relation relation = scan->heap_rel;
    TupleTableSlot *slot = scan->slot;
    BufferHeapTupleTableSlot *buffer_slot = (BufferHeapTupleTableSlot *) slot;

    // Get the original tuple and its buffer from the slot
    HeapTuple original_tuple = buffer_slot->base.tuple;
    Buffer buffer = buffer_slot->buffer;

    // Perform the actual in-place update and unlock the buffer
    heap_inplace_update_and_unlock(relation, original_tuple, tuple, buffer);

    // Clean up the scan state
    systable_endscan(scan);
}
```

Key simplifications made:
- Added descriptive variable names (buffer_slot, original_tuple)
- Added explanatory comments for each major step
- Clarified the casting operation with a comment
- Emphasized the two main operations: update and cleanup
- Maintained the exact logic flow and all critical operations