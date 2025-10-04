# proclist_pop_head_node_offset

## Location
[src/include/storage/proclist.h:173-186](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/proclist.h#L173-L186)

## Overview
A static inline function that removes and returns the first process from a process list at a specified node offset.

## Definition
```c
static inline PGPROC *proclist_pop_head_node_offset(proclist_head *list, size_t node_offset)
```

## Detailed Description
This function combines list removal and process retrieval operations by removing the head process from a process list and returning a pointer to the corresponding PGPROC structure. It first verifies that the list is not empty, retrieves the process structure for the head node, removes that process from the list using the offset-based deletion function, and finally returns the process pointer. This is a common operation pattern for process list management where you need both the process object and list modification in a single atomic operation.

## Parameters / Member Variables
- `list`: Pointer to the process list head structure from which to pop the first process
- `node_offset`: Byte offset within the process structure where the proclist_node is located

## Dependencies
- Functions called/Symbols referenced:
  - [proclist_head](proclist_head.md) (list header structure)
  - [PGPROC](../P/PGPROC.md) (process structure type)
  - [proclist_is_empty](proclist_is_empty.md) (to verify list has elements)
  - GetPGProcByNumber (to convert process number to PGPROC pointer)
  - [proclist_delete_offset](proclist_delete_offset.md) (to remove the process from the list)
- Called from (representative examples):
  - proclist_pop_head_node

## Notes and Other Information
- Assumes the list is not empty (verified by assertion)
- Combines retrieval and removal operations for efficiency
- Returns a valid PGPROC pointer to the process that was at the head of the list
- The returned process is no longer part of the list after this operation
- Commonly used in process scheduling and queue management scenarios

## Simplified Source

```c
static inline PGPROC *proclist_pop_head_node_offset(proclist_head *list, size_t node_offset) {
    // Ensure list has at least one element
    Assert(!proclist_is_empty(list));

    // Get the process at the head
    PGPROC *proc = GetPGProcByNumber(list->head);

    // Remove it from the list
    proclist_delete_offset(list, list->head, node_offset);

    return proc;
}
```

**Simplified Logic:**
1. **Safety Check**: Verifies the list is not empty before attempting to pop
2. **Retrieve Process**: Gets the PGPROC structure for the head process
3. **Remove from List**: Deletes the head process from the list using offset-based deletion
4. **Return Process**: Returns pointer to the retrieved process