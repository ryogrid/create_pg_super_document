# slist_pop_head_node

## Location
[src/include/lib/ilist.h:1028-1042](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/ilist.h#L1028-L1042)

## Overview
Removes and returns the first node from a singly linked list in PostgreSQL's intrusive list implementation.

## Definition

```c
static inline slist_node *
slist_pop_head_node(slist_head *head)
```
## Detailed Description
This function implements the standard "pop from head" operation for PostgreSQL's singly linked list data structure. It removes the first node from the list and returns a pointer to that node for the caller to process. The function ensures list integrity by updating the head's next pointer to point to what was previously the second node in the list.

The operation runs in O(1) constant time and includes safety checks to ensure the list is not empty before attempting the pop operation. After the removal, the function validates the list state using slist_check() to maintain data structure integrity. This is a destructive operation that modifies the list structure.

## Parameters / Member Variables
- : Pointer to the list head structure from which to remove the first node

## Dependencies
- Functions called/Symbols referenced:
  - [slist_is_empty](slist_is_empty.md) (to verify list is not empty before popping)
  - [slist_check](slist_check.md) (for list integrity validation)
- Data types used:
  - [slist_head](slist_head.md)
  - [slist_node](slist_node.md)
- Called from (representative examples):
  - [dsm_detach](../d/dsm_detach.md) (src/backend/storage/ipc/dsm.c:821)
  - [reset_on_dsm_detach](../r/reset_on_dsm_detach.md) (src/backend/storage/ipc/dsm.c:1184)

## Notes and Other Information
- This is an inline function for maximum performance in list operations  
- The function requires that the list contains at least one node (enforced by Assert)
- The caller is responsible for handling the returned node (e.g., processing its data, freeing memory)
- The returned node is no longer part of the list after this operation
- [List](../L/List.md) integrity is validated through slist_check() in debug builds
- Part of PostgreSQL's efficient intrusive list implementation
- The function will cause assertion failure in debug builds if called on an empty list
- Primarily used in dynamic shared memory (DSM) management for cleanup operations

## Simplified Source

```c
// Simplified version of slist_pop_head_node
static inline slist_node *
slist_pop_head_node(slist_head *head)
{
    slist_node *node;

    // Safety check: ensure list is not empty
    Assert(!slist_is_empty(head));

    // Get the first node and update head to point to second node
    node = head->head.next;
    head->head.next = node->next;

    // Validate list integrity in debug builds
    slist_check(head);

    return node;
}
```

Key simplifications made:
- Added descriptive comments explaining each logical step
- Preserved all original functionality as the function was already quite simple
- Maintained the essential Assert safety check and integrity validation
- No significant logic changes needed due to the function's straightforward implementation