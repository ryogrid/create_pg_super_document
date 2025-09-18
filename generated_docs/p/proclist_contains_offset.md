# proclist_contains_offset

## Location
src/include/storage/proclist.h: 146 - 172

## Overview
A static inline function that checks whether a specific process is currently contained in a process list at a specified node offset.

## Definition
```c
static inline bool proclist_contains_offset(const proclist_head *list, int procno, size_t node_offset)
```

## Detailed Description
This function determines if a given process is currently a member of a specific process list. It operates under the assumption that the process is not simultaneously in any other process list that uses the same proclist_node structure. The function performs a quick check by examining the node's prev/next pointers and includes assertion-based validation for head and tail positions to catch potential errors during development. The design prioritizes performance by avoiding full list traversal, making it suitable for use in performance-critical sections such as while holding spinlocks.

## Parameters / Member Variables
- `list`: Pointer to the process list head structure to check membership against
- `procno`: Process number (identifier) of the process to check for membership
- `node_offset`: Byte offset within the process structure where the proclist_node is located

## Dependencies
- Functions called/Symbols referenced:
  - proclist_node_get (to access node structures at specified offsets)
  - proclist_head (list header structure)
  - proclist_node (node structure within processes)
  - INVALID_PROC_NUMBER (constant indicating invalid process number)
  - PGPROC (process structure type)
- Called from (representative examples):
  - proclist_contains

## Notes and Other Information
- Assumes the process is not in any other list using the same node structure
- Returns false immediately if the node has null prev/next pointers (not in any list)
- Uses O(1) assertions to verify head/tail consistency without full list traversal
- Designed for use in performance-critical code paths, including spinlock-protected sections
- The function prioritizes speed over comprehensive validation due to typical usage patterns