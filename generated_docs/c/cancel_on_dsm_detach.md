# cancel_on_dsm_detach

## Location
[src/backend/storage/ipc/dsm.c:1147-1169](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/dsm.c#L1147-L1169)

## Overview
Removes a previously registered callback function from a dynamic shared memory segment's detach callback list.

## Definition
```c
void cancel_on_dsm_detach(dsm_segment *seg, on_dsm_detach_callback function, Datum arg)
```

## Detailed Description
This function allows processes to unregister callback functions that were previously registered with on_dsm_detach(). It searches through the segment's callback list to find a callback that matches both the function pointer and argument, then removes and frees that callback entry.

The function uses a safe iteration mechanism (slist_foreach_modify) that allows modification of the list during iteration. When a matching callback is found (based on both the function pointer and the Datum argument), it is removed from the list using slist_delete_current() and the associated memory is freed with pfree().

This functionality is important for scenarios where a callback registration needs to be cancelled before the segment is detached, such as when cleaning up resources manually or when error conditions require early cleanup.

## Parameters / Member Variables
- `seg`: Pointer to the dsm_segment structure from which to remove the callback
- `function`: The callback function to be removed (must match exactly)
- `arg`: The Datum argument that was registered with the callback (must match exactly)

## Dependencies
- Functions called/Symbols referenced:
  - [dsm_segment](../d/dsm_segment.md) (structure type)
  - [slist_mutable_iter](../s/slist_mutable_iter.md) (iterator type)
  - slist_foreach_modify (linked list iteration macro)
  - [dsm_segment_detach_callback](../d/dsm_segment_detach_callback.md) (structure type)
  - slist_container (macro to get container from list node)
  - [slist_delete_current](../s/slist_delete_current.md) (list modification function)
  - [pfree](../p/pfree.md) (memory deallocation function)
- Called from (representative examples):
  - [shm_mq_detach](../s/shm_mq_detach.md)
  - [test_shm_mq_setup](../t/test_shm_mq_setup.md)

## Notes and Other Information
- Both the function pointer and argument must match exactly for a callback to be removed

## Simplified Source

```c
void cancel_on_dsm_detach(dsm_segment *seg, on_dsm_detach_callback function, Datum arg)
{
    slist_mutable_iter iter;

    slist_foreach_modify(iter, &seg->on_detach)
    {
        dsm_segment_detach_callback *cb;

        cb = slist_container(dsm_segment_detach_callback, node, iter.cur);
        if (cb->function == function && cb->arg == arg)
        {
            slist_delete_current(&iter);
            pfree(cb);
            break;
        }
    }
}
```
- Only the first matching callback is removed (the function breaks after finding a match)
- The function uses safe iteration that allows list modification during traversal
- Memory allocated for the callback structure is properly freed with pfree()
- This provides a way to cancel callbacks before segment detachment occurs
- Less commonly used than on_dsm_detach(), typically only needed in specific cleanup scenarios
- The matching is done by pointer equality for both function and arg parameters