# proclist_node_get

## Location
src/include/storage/proclist.h: 48 - 58

## Overview
Retrieves a pointer to a proclist_node structure within a PGPROC structure using a process number and field offset.

## Definition
```c
static inline proclist_node *proclist_node_get(int procno, size_t node_offset)
```

## Detailed Description
The `proclist_node_get` function is a utility function that provides access to a proclist_node structure embedded within a PGPROC (PostgreSQL process) structure. Given a process number and the byte offset of the proclist_node field within the PGPROC structure, it returns a pointer to that specific proclist_node.

This function is essential for the proclist infrastructure because it allows generic proclist operations to work with proclist_node fields that may be located at different offsets within PGPROC structures. The function first retrieves the PGPROC structure using `GetPGProcByNumber`, then performs pointer arithmetic to locate the specific proclist_node field.

Being defined as static inline, this function is optimized for performance since it's called frequently by other proclist operations.

## Parameters / Member Variables
- `procno`: The process number identifying which PGPROC structure to access
- `node_offset`: The byte offset of the proclist_node field within the PGPROC structure

## Dependencies
- Functions called/Symbols referenced:
  - `GetPGProcByNumber` (function to retrieve PGPROC structure by process number)
  - `proclist_node` (data structure type)
- Called from (representative examples):
  - `[proclist_push_head_offset](proclist_push_head_offset.md)` (src/include/storage/proclist.h:61, 77)
  - `[proclist_push_tail_offset](proclist_push_tail_offset.md)` (src/include/storage/proclist.h:89, 105)
  - `[proclist_delete_offset](proclist_delete_offset.md)` (src/include/storage/proclist.h:117, 127, 135)
  - `[proclist_contains_offset](proclist_contains_offset.md)` (src/include/storage/proclist.h:149)
  - `proclist_foreach_modify` (src/include/storage/proclist.h:211, 216)

## Notes and Other Information
- This is a static inline function defined in the header file for optimal performance
- The function enables type-safe access to proclist_node fields at arbitrary offsets within PGPROC structures
- It's a fundamental building block used by higher-level proclist operations like push, delete, and iteration
- The offset-based approach allows the same proclist code to work with different proclist_node fields in PGPROC
- The function assumes the procno parameter refers to a valid, active process