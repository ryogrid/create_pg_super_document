# dlist_delete_from

## Location
[src/include/lib/ilist.h:429-439](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/ilist.h#L429-L439)

## Overview
Removes a node from a specific doubly-linked list with optional debug validation to ensure the node actually belongs to the specified list.

## Definition
```c
static inline void dlist_delete_from(dlist_head *head, dlist_node *node)
```

## Detailed Description
This function provides a safer version of dlist_delete by accepting both the list head and the node to be deleted. In debug builds (when ILIST_DEBUG is defined), it performs validation to ensure that the specified node actually belongs to the given list before proceeding with the deletion. In non-debug builds, it simply calls dlist_delete after the validation check.

The function first calls dlist_member_check to validate the node membership (in debug builds), then proceeds with the standard deletion using dlist_delete. This approach helps catch programming errors where a node might be deleted from the wrong list or when it's not actually a member of the expected list.

## Parameters / Member Variables
- `head`: Pointer to the head of the doubly-linked list from which the node should be deleted
- `node`: Pointer to the node to be removed from the list

## Dependencies
- Functions called/Symbols referenced:
  - [dlist_head](dlist_head.md) (data structure)
  - [dlist_node](dlist_node.md) (data structure)
  - [dlist_member_check](dlist_member_check.md) (validation function)
  - [dlist_delete](dlist_delete.md) (core deletion function)
- Called from (representative examples):
  - [SlabAlloc](../S/SlabAlloc.md) (src/backend/utils/mmgr/slab.c:685)
  - [SlabFree](../S/SlabFree.md) (src/backend/utils/mmgr/slab.c:755, 781)
  - [dclist_delete_from](dclist_delete_from.md) (src/include/lib/ilist.h:767)

## Notes and Other Information
- This is an inline function for performance optimization
- The dlist_member_check validation only occurs in debug builds (ILIST_DEBUG defined)
- In production builds, the member check is compiled out for performance
- Provides an additional layer of safety for list operations by validating node membership
- Particularly useful in memory management contexts like the slab allocator
- The function helps prevent bugs related to deleting nodes from incorrect lists
- After validation, it delegates the actual deletion to the standard dlist_delete function
- Used primarily in scenarios where list integrity is critical and debugging support is beneficial
- The head parameter is only used for validation purposes, not for the actual deletion operation