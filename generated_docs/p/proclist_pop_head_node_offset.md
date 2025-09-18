# proclist_pop_head_node_offset

## Location
src/include/storage/proclist.h: 173 - 186

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
  - proclist_head (list header structure)
  - PGPROC (process structure type)
  - proclist_is_empty (to verify list has elements)
  - GetPGProcByNumber (to convert process number to PGPROC pointer)
  - proclist_delete_offset (to remove the process from the list)
- Called from (representative examples):
  - proclist_pop_head_node

## Notes and Other Information
- Assumes the list is not empty (verified by assertion)
- Combines retrieval and removal operations for efficiency
- Returns a valid PGPROC pointer to the process that was at the head of the list
- The returned process is no longer part of the list after this operation
- Commonly used in process scheduling and queue management scenarios