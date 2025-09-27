# dlist_check

## Location
[src/backend/lib/ilist.c:77-113](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/ilist.c#L77-L113)

## Overview
Performs comprehensive integrity validation of a doubly-linked list by checking structural consistency in both forward and backward directions.

## Definition
```c
void dlist_check(const dlist_head *head)
```

## Detailed Description
The `dlist_check` function provides thorough validation of a doubly-linked list's structural integrity. It performs multiple consistency checks to ensure the list is in a valid state and can be safely traversed or manipulated.

The function first validates that the head pointer is not NULL and handles the special case of an empty list (where both next and prev pointers of the head are NULL). For non-empty lists, it performs bidirectional traversal - first iterating forward from head.next to the head, then backward from head.prev to the head. During each traversal, it validates that:

- No node pointers are NULL
- Each node's next and prev pointers are valid
- The linking is consistent (cur->prev->next == cur and cur->next->prev == cur)
- The circular structure is maintained

If any inconsistency is detected, the function raises an ERROR using elog, which will abort the current transaction.

## Parameters / Member Variables
- `head`: Pointer to the doubly-linked list head structure to be validated (const-qualified as this is a read-only validation operation)

## Dependencies
- Functions called/Symbols referenced:
  - elog (for error reporting)
- Called from (representative examples):
  - [dlist_is_empty](dlist_is_empty.md)
  - [dlist_push_head](dlist_push_head.md)
  - [dlist_push_tail](dlist_push_tail.md)
  - [dlist_move_head](dlist_move_head.md)
  - [dlist_move_tail](dlist_move_tail.md)

## Notes and Other Information
- **Performance**: This is an O(n) operation that traverses the entire list twice (forward and backward), making it expensive for large lists
- **Error Handling**: Raises ERROR (not warning) for any corruption detected, causing transaction abort in PostgreSQL context
- **Validation Scope**: Checks both structural integrity (NULL pointers) and logical consistency (bidirectional linking)
- **Empty List Handling**: Correctly handles the case where both head.next and head.prev are NULL (representing an empty, zero-initialized list)
- **Debug Usage**: Primarily used in debug builds and during development to catch list corruption early
- **Thread Safety**: Read-only operation that should be safe for concurrent access, though external synchronization may be needed depending on usage context

## Simplified Source

```c
// Simplified version of dlist_check
void dlist_check(const dlist_head *head) {
    dlist_node *cur;

    // Basic validation: ensure head is not NULL
    if (head == NULL) {
        elog(ERROR, "doubly linked list head address is NULL");
    }

    // Handle empty list case
    if (head->head.next == NULL && head->head.prev == NULL) {
        return; // Empty list is valid
    }

    // Forward traversal: check each node's integrity
    for (cur = head->head.next; cur != &head->head; cur = cur->next) {
        // Validate node pointers and bidirectional linking
        if (cur == NULL || cur->next == NULL || cur->prev == NULL ||
            cur->prev->next != cur || cur->next->prev != cur) {
            elog(ERROR, "doubly linked list is corrupted");
        }
    }

    // Backward traversal: verify consistency in reverse direction
    for (cur = head->head.prev; cur != &head->head; cur = cur->prev) {
        // Same integrity checks in reverse direction
        if (cur == NULL || cur->next == NULL || cur->prev == NULL ||
            cur->prev->next != cur || cur->next->prev != cur) {
            elog(ERROR, "doubly linked list is corrupted");
        }
    }
}
```

Key simplifications made:
- Consolidated error conditions into clearer logical groups
- Added explanatory comments for each major validation phase
- Maintained the essential two-pass validation (forward and backward)
- Preserved all critical error detection logic
- Simplified variable naming and spacing for better readability