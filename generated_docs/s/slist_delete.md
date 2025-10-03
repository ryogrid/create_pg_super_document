# slist_delete

## Location
[src/backend/lib/ilist.c:31-59](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/ilist.c#L31-L59)

## Overview
Removes a specified node from a singly-linked list by traversing the list to find and unlink the node.

## Definition

```c
void
slist_delete(slist_head *head, const slist_node *node)
```
## Detailed Description
The  function removes a node from a singly-linked list by performing a linear search to locate the specified node and then unlinking it. The function traverses the list starting from the head, maintaining a pointer to the previous node to enable proper relinking when the target node is found. This operation has O(n) time complexity as it may need to traverse the entire list to find the node.

The function includes assertion checking to ensure that the specified node is actually found in the list, which helps catch programming errors where attempts are made to delete nodes that don't belong to the list. After deletion, it calls  to validate the list's integrity in debug builds.

## Parameters / Member Variables
- `*head`: Pointer to the singly-linked list head structure that manages the list
- `*node`: Pointer to the node to be removed from the list (must be an existing member of the list)
## Dependencies
- Functions called/Symbols referenced:
  - [slist_check](slist_check.md)
  - Assert (macro)
- Called from (representative examples):
  - [RemoveGUCFromLists](../R/RemoveGUCFromLists.md)
  - [reapply_stacked_values](../r/reapply_stacked_values.md)

## Notes and Other Information
- **Performance Warning**: This is an O(n) operation that requires traversing the list from the beginning. For better performance when the current position is known, consider using  instead
- **Precondition**: The node must actually exist in the specified list; attempting to delete a node not in the list will trigger an assertion failure in debug builds
- **Memory Management**: This function only unlinks the node from the list; it does not free the node's memory - that responsibility lies with the caller
- **Thread Safety**: This function is not thread-safe and requires external synchronization if used in multi-threaded contexts