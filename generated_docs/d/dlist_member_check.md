# dlist_member_check

## Location
[src/backend/lib/ilist.c:60-76](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/ilist.c#L60-L76)

## Overview
Validates that a specified node is actually a member of a doubly-linked list by traversing the entire list to find the node.

## Definition
```c
void dlist_member_check(const dlist_head *head, const dlist_node *node)
```

## Detailed Description
The `dlist_member_check` function performs a membership validation by traversing a doubly-linked list to verify that a given node is actually part of the specified list. This is primarily a debugging and validation function that helps catch programming errors where operations are attempted on nodes that don't belong to the expected list.

The function iterates through the entire list starting from the head's next pointer and continuing until it circles back to the head node (which marks the end of the circular structure). If the specified node is found during traversal, the function returns successfully. If the entire list is traversed without finding the node, it raises an ERROR using elog, indicating a membership check failure.

## Parameters / Member Variables
- `head`: Pointer to the doubly-linked list head structure (const-qualified as this is a read-only operation)
- `node`: Pointer to the node being validated for membership in the list (const-qualified)

## Dependencies
- Functions called/Symbols referenced:
  - elog (for error reporting)
- Called from (representative examples):
  - [dlist_delete_from](dlist_delete_from.md)
  - [dlist_delete_from_thoroughly](dlist_delete_from_thoroughly.md)
  - [dclist_insert_after](dclist_insert_after.md)
  - [dclist_insert_before](dclist_insert_before.md)
  - [dclist_move_head](dclist_move_head.md)
  - [dclist_move_tail](dclist_move_tail.md)

## Notes and Other Information
- **Performance**: This is an O(n) operation as it may need to traverse the entire list to find the node or determine it's not present
- **Error Handling**: Raises an ERROR (not just a warning) if the node is not found, which will abort the current transaction in PostgreSQL
- **Debug Purpose**: This function is primarily used for validation and debugging, often called by other list manipulation functions when built with assertion checking enabled
- **Const Correctness**: Both parameters are const-qualified since this function only reads the list structure without modifying it
- **Thread Safety**: Read-only operation that should be safe for concurrent access, but external synchronization may still be needed depending on the context

## Simplified Source

```c
void
dlist_member_check(const dlist_head *head, const dlist_node *node)
{
    const dlist_node *cur;

    // Traverse the entire list to find the node
    for (cur = head->head.next; cur != &head->head; cur = cur->next) {
        if (cur == node)
            return;  // Node found
    }

    // Node not found - report error
    elog(ERROR, "double linked list member check failure");
}
```