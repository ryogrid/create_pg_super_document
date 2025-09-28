# enlarge_list

## Location
[src/backend/nodes/list.c:155-235](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/list.c#L155-L235)

## Overview
A static function that expands the capacity of an existing non-NIL List to accommodate at least the specified minimum number of cells, handling both initial inline storage and separate allocation scenarios.

## Definition
```c
static void enlarge_list(List *list, int min_size)
```

## Detailed Description
This function increases the storage capacity of an existing List when more space is needed. It handles two distinct scenarios: lists using their initial inline storage (initial_elements) and lists that have already been moved to separate allocated storage. For inline storage, it allocates a new separate block and copies existing data, maintaining the same memory context as the List header. For separate storage, it uses repalloc() to resize the existing allocation. The function implements power-of-2 allocation sizing in normal builds (minimum 16 cells) and exact sizing in debug builds. Importantly, it does not update the list's length field, leaving that responsibility to the caller.

## Parameters / Member Variables
- `list`: Pointer to the existing List structure to be enlarged. Must be non-NIL.
- `min_size`: The minimum number of cells that the list must be able to hold after enlargement. Must be greater than the current max_length.

## Dependencies
- Functions called/Symbols referenced:
  - Assert (assertion macro)
  - [pg_nextpower2_32](../p/pg_nextpower2_32.md) (calculates next power of 2)
  - [GetMemoryChunkContext](../G/GetMemoryChunkContext.md) (gets memory context of allocation)
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md) (allocates memory in specific context)
  - memcpy (copies memory)
  - [repalloc](../r/repalloc.md) (reallocates memory)
  - [pfree](../p/pfree.md) (frees memory)
  - [wipe_mem](../w/wipe_mem.md) (debug memory clearing)
  - VALGRIND_MAKE_MEM_NOACCESS (Valgrind debugging macro)
  - Max (maximum value macro)
  - DEBUG_LIST_MEMORY_USAGE (conditional compilation flag)
  - CLOBBER_FREED_MEMORY (debug memory clobbering flag)

- Called from (representative examples):
  - [new_head_cell](../n/new_head_cell.md)
  - [new_tail_cell](../n/new_tail_cell.md)
  - [insert_new_cell](../i/insert_new_cell.md)
  - [list_concat](../l/list_concat.md)

## Notes and Other Information
- Static function internal to list.c, not part of the public API
- Handles transition from inline storage to separate allocation gracefully
- Maintains memory context consistency between List header and elements
- Uses different allocation strategies based on compilation flags:
  - Normal builds: power-of-2 sizing with 16-cell minimum for growth efficiency
  - Debug builds: exact allocation to test memory management thoroughly
- Does not update list->length, allowing callers to manage length separately
- Includes extensive debugging support with memory access controls and wiping
- The function cannot reclaim initial_elements space due to header immobility constraints
- Optimizes for both memory efficiency and debugging capabilities

## Simplified Source

```c
// Simplified version of enlarge_list
static void enlarge_list(List *list, int min_size) {
    int new_max_len;

    // Calculate new capacity - use power of 2 for efficiency, minimum 16
    new_max_len = pg_nextpower2_32(Max(16, min_size));

    if (list->elements == list->initial_elements) {
        // First expansion: transition from inline storage to separate allocation
        // Allocate new block in same memory context as list header
        list->elements = (ListCell *)
            MemoryContextAlloc(GetMemoryChunkContext(list),
                             new_max_len * sizeof(ListCell));

        // Copy existing data from inline storage
        memcpy(list->elements, list->initial_elements,
               list->length * sizeof(ListCell));

        // Mark old inline storage as inaccessible (debug builds)
    } else {
        // Subsequent expansion: resize existing separate allocation
        list->elements = (ListCell *) repalloc(list->elements,
                                             new_max_len * sizeof(ListCell));
    }

    // Update capacity (caller manages length separately)
    list->max_length = new_max_len;
}
```

Key simplifications made:
- Removed debug-specific conditional compilation blocks for clarity
- Simplified memory debugging code to high-level comments
- Consolidated the two allocation paths with clear explanations
- Removed detailed error handling and memory wiping code
- Focused on the essential algorithm: calculate new size, handle inline vs separate storage, allocate/copy/update
- Preserved the core logic of power-of-2 sizing and memory context consistency