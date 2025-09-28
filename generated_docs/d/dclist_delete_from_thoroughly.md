# dclist_delete_from_thoroughly

## Location
[src/include/lib/ilist.h:776-788](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/ilist.h#L776-L788)

## Overview
Removes a node from a doubly-linked counted list and sets the node's next/prev pointers to NULL to indicate it's no longer part of any list.

## Definition

```c
static inline void
dclist_delete_from_thoroughly(dclist_head *head, dlist_node *node)
```
## Detailed Description
This function extends the functionality of  by not only removing a node from the doubly-linked counted list but also nullifying the node's next and previous pointers. This provides a clear signal that the node is not currently part of any list, which can be useful for debugging and preventing accidental reuse of nodes that are meant to be detached.

The function decrements the list's count after performing the deletion, maintaining the counted list's integrity. It includes an assertion to ensure the list is not empty before attempting deletion.

## Parameters / Member Variables
- : Pointer to the counted list head structure from which to remove the node
- : Pointer to the list node to be removed and thoroughly cleaned

## Dependencies
- Functions called/Symbols referenced:
  - [dlist_delete_from_thoroughly](dlist_delete_from_thoroughly.md)
  - [dclist_head](dclist_head.md) (structure type)
  - [dlist_node](dlist_node.md) (structure type)
- Called from (representative examples):
  - [RemoveFromWaitQueue](../R/RemoveFromWaitQueue.md) (src/backend/storage/lmgr/lock.c:1923)
  - [ProcWakeup](../P/ProcWakeup.md) (src/backend/storage/lmgr/proc.c:1691)

## Notes and Other Information
- This is an inline function defined in the header file for performance
- The 'thoroughly' variant ensures the removed node's pointers are nullified, making it safer for debugging
- Includes an assertion to verify the list count is greater than zero before deletion
- Used primarily in lock management and process waiting queue scenarios

## Simplified Source

```c
// Simplified version of dclist_delete_from_thoroughly
static inline void dclist_delete_from_thoroughly(dclist_head *head, dlist_node *node) {
    // Ensure list is not empty before deletion
    Assert(head->count > 0);

    // Remove node and nullify its pointers for safety
    dlist_delete_from_thoroughly(&head->dlist, node);

    // Decrement the count to maintain list integrity
    head->count--;
}
```

Key simplifications made:
- Added explanatory comments for each operation
- Preserved the safety assertion for non-empty list
- Emphasized the pointer nullification benefit
- Maintained the count decrement for list integrity