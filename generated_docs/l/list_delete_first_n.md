# list_delete_first_n

## Location
[src/backend/nodes/list.c:983-1065](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/list.c#L983-L1065)

## Overview
Removes the first N elements from a PostgreSQL List, providing bulk deletion functionality with optimized memory management.

## Definition

```c
List *
list_delete_first_n(List *list, int n)
```
## Detailed Description
This function efficiently removes the first N elements from a PostgreSQL List data structure. It handles various edge cases and provides different memory management strategies depending on compilation flags.

For no-op requests (n <= 0), the function returns the original list unchanged. If N is greater than or equal to the list length, the entire list is freed and NIL is returned. For partial deletions, the function uses different strategies:

In normal operation (DEBUG_LIST_MEMORY_USAGE not defined), it uses memmove() to shift remaining elements to the beginning of the array and updates the length. This is efficient but reuses the same memory.

In debug mode (DEBUG_LIST_MEMORY_USAGE defined), it allocates new memory for the remaining elements and copies them over, then properly handles freeing the old memory. This helps detect memory usage issues but is less efficient.

The function maintains proper memory context management and includes provisions for memory debugging tools like Valgrind.

## Parameters / Member Variables
- : The PostgreSQL List from which to remove the first N elements. Can be NIL (empty list).
- : The number of elements to remove from the beginning of the list. Non-positive values result in no-op.

## Dependencies
- Functions called/Symbols referenced:
  - [check_list_invariants](../c/check_list_invariants.md): Validates list structure integrity (called twice in debug mode)
  - [list_length](list_length.md): Determines if the entire list should be deleted
  - [list_free](list_free.md): Deallocates the entire list when all elements are removed
  - memmove: Shifts remaining elements in normal mode
  - [GetMemoryChunkContext](../G/GetMemoryChunkContext.md): Gets the memory context for new allocations in debug mode
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md): Allocates new memory for remaining elements in debug mode
  - [wipe_mem](../w/wipe_mem.md): Clears freed memory in debug mode with CLOBBER_FREED_MEMORY
  - VALGRIND_MAKE_MEM_NOACCESS: Marks memory as inaccessible for Valgrind in debug mode
- Called from (representative examples):
  - [add_function_defaults](../a/add_function_defaults.md): Function default argument processing
  - [func_get_detail](../f/func_get_detail.md): Function signature resolution
  - [SyncPostCheckpoint](../S/SyncPostCheckpoint.md): Post-checkpoint synchronization

## Notes and Other Information
- Performance complexity: O(list_length - n) due to element shifting
- Handles no-op requests gracefully (n <= 0)
- Automatically frees the entire list if all elements would be removed
- Includes comprehensive debug mode with separate memory allocation strategy
- Debug mode helps detect memory corruption and usage issues
- Memory context preservation ensures proper PostgreSQL memory management
- More efficient than calling list_delete_first() in a loop for multiple deletions
- Commonly used in optimization and parsing contexts where bulk removal is needed
- The debug mode includes special handling for initial_elements vs dynamically allocated elements

## Simplified Source

```c
// Simplified version of list_delete_first_n
List *list_delete_first_n(List *list, int n) {
    // Handle edge cases
    if (n <= 0)
        return list;  // No-op request

    if (n >= list_length(list)) {
        list_free(list);
        return NIL;  // Delete entire list
    }

    // Normal case: remove first n elements
#ifndef DEBUG_LIST_MEMORY_USAGE
    // Efficient approach: shift remaining elements to beginning
    memmove(&list->elements[0], &list->elements[n],
            (list->length - n) * sizeof(ListCell));
    list->length -= n;
#else
    // Debug approach: allocate new memory for remaining elements
    ListCell *newelems;
    int newmaxlen = list->length - n;

    newelems = (ListCell *) MemoryContextAlloc(GetMemoryChunkContext(list),
                                               newmaxlen * sizeof(ListCell));
    memcpy(newelems, &list->elements[n], newmaxlen * sizeof(ListCell));

    // Handle old memory cleanup
    if (list->elements != list->initial_elements)
        pfree(list->elements);

    list->elements = newelems;
    list->max_length = newmaxlen;
    list->length = newmaxlen;
#endif

    return list;
}
```

Key simplifications made:
- Streamlined the edge case handling logic
- Preserved both normal and debug memory management strategies
- Removed detailed memory debugging instrumentation for clarity
- Focused on the core algorithm: handle special cases → shift/copy remaining elements
- Maintained proper memory context management