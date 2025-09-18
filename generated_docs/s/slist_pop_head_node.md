# slist_pop_head_node

## Location
src/include/lib/ilist.h: 1028 - 1042

## Overview
Removes and returns the first node from a singly linked list in PostgreSQL's intrusive list implementation.

## Definition


## Detailed Description
This function implements the standard "pop from head" operation for PostgreSQL's singly linked list data structure. It removes the first node from the list and returns a pointer to that node for the caller to process. The function ensures list integrity by updating the head's next pointer to point to what was previously the second node in the list.

The operation runs in O(1) constant time and includes safety checks to ensure the list is not empty before attempting the pop operation. After the removal, the function validates the list state using slist_check() to maintain data structure integrity. This is a destructive operation that modifies the list structure.

## Parameters / Member Variables
- : Pointer to the list head structure from which to remove the first node

## Dependencies
- Functions called/Symbols referenced:
  - slist_is_empty (to verify list is not empty before popping)
  - slist_check (for list integrity validation)
- Data types used:
  - slist_head
  - slist_node
- Called from (representative examples):
  - dsm_detach (src/backend/storage/ipc/dsm.c:821)
  - reset_on_dsm_detach (src/backend/storage/ipc/dsm.c:1184)

## Notes and Other Information
- This is an inline function for maximum performance in list operations  
- The function requires that the list contains at least one node (enforced by Assert)
- The caller is responsible for handling the returned node (e.g., processing its data, freeing memory)
- The returned node is no longer part of the list after this operation
- List integrity is validated through slist_check() in debug builds
- Part of PostgreSQL's efficient intrusive list implementation
- The function will cause assertion failure in debug builds if called on an empty list
- Primarily used in dynamic shared memory (DSM) management for cleanup operations