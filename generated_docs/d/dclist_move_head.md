# dclist_move_head

## Location
[src/include/lib/ilist.h:808-823](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/ilist.h#L808-L823)

## Overview
Moves a node from its current position within a doubly-linked counted list to the head (first) position of the same list.

## Definition
```c
static inline void dclist_move_head(dclist_head *head, dlist_node *node)
```

## Detailed Description
This function relocates an existing node within a doubly-linked counted list to the head position without affecting the list's count. The node must already be a member of the specified list. The function includes membership verification and assertion checks to ensure the operation is valid.

This operation is commonly used in cache management and LRU (Least Recently Used) algorithms where recently accessed items need to be moved to the front of the list for efficient access patterns.

## Parameters / Member Variables
- `head`: Pointer to the counted list head structure containing the node
- `node`: Pointer to the existing list node to be moved to the head position

## Dependencies
- Functions called/Symbols referenced:
  - [dlist_member_check](dlist_member_check.md)
  - [dlist_move_head](dlist_move_head.md)
  - [dclist_head](dclist_head.md) (structure type)  
  - [dlist_node](dlist_node.md) (structure type)
- Called from (representative examples):
  - [mXactCacheGetBySet](../m/mXactCacheGetBySet.md) (src/backend/access/transam/multixact.c:1636)
  - [mXactCacheGetById](../m/mXactCacheGetById.md) (src/backend/access/transam/multixact.c:1685)

## Notes and Other Information
- This is an inline function defined in the header file for performance
- Includes membership verification via dlist_member_check to ensure the node belongs to the list
- The list count remains unchanged since this is a reordering operation, not insertion/deletion
- Commonly used in transaction management for multixact caching
- Caution: The node must already be a member of the specified list before calling this function