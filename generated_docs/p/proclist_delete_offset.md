# proclist_delete_offset

## Location
src/include/storage/proclist.h: 115 - 145

## Overview
A static inline function that removes a process from a process list at a specified node offset within the process structure.

## Definition


## Detailed Description
This function removes a specified process from a doubly-linked process list. It operates on process lists where the list nodes are embedded at a specific offset within the process structure, allowing for multiple different process lists to coexist within the same process objects. The function performs proper list maintenance by updating the previous and next pointers of adjacent nodes, and handles special cases for head and tail nodes. After removal, the node's pointers are reset to indicate it's no longer part of any list.

## Parameters / Member Variables
- : Pointer to the process list head structure from which to remove the process
- : Process number (identifier) of the process to be removed from the list
- : Byte offset within the process structure where the proclist_node is located

## Dependencies
- Functions called/Symbols referenced:
  - proclist_node_get (to access node structures at specified offsets)
  - proclist_head (list header structure)
  - proclist_node (node structure within processes)
  - INVALID_PROC_NUMBER (constant indicating invalid process number)
- Called from (representative examples):
  - proclist_pop_head_node_offset
  - proclist_delete

## Notes and Other Information
- The function assumes the process is actually in the list (assertion checks verify this)
- Handles both head and tail removal cases properly
- Resets the removed node's pointers to 0 to indicate it's no longer in any list
- Uses assertions to verify list consistency during removal
- This is a low-level function typically used by higher-level list management functions