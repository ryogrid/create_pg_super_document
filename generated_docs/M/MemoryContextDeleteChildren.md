# MemoryContextDeleteChildren

## Location
[src/backend/utils/mmgr/mcxt.c:539-567](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/mcxt.c#L539-L567)

## Overview
Deletes all descendants of the named context and releases all space allocated therein, while leaving the named context itself untouched.

## Definition

```c
void
MemoryContextDeleteChildren(MemoryContext context)
```
## Detailed Description
MemoryContextDeleteChildren provides a targeted deletion operation that removes all child contexts of a specified parent context while preserving the parent itself. This function is particularly useful when you need to clear all subordinate memory contexts but maintain the parent structure for continued use.

The implementation uses a simple but effective approach: it repeatedly deletes the first child until no children remain. This works because MemoryContextDelete automatically delinks each child from its parent during deletion, causing the next sibling to become the new first child. The loop continues until the firstchild pointer becomes NULL, indicating all children have been removed.

This function is commonly used in cleanup scenarios where the main context needs to persist but all its temporary or subsidiary contexts should be removed.

## Parameters / Member Variables
- `context`: The parent memory context whose children will be deleted. The context itself remains valid and untouched.
## Dependencies
- Functions called/Symbols referenced:
  - MemoryContextIsValid
  - [MemoryContextDelete](MemoryContextDelete.md)
- Called from (representative examples):
  - [PersistHoldablePortal](../P/PersistHoldablePortal.md)
  - [PortalRunMulti](../P/PortalRunMulti.md)
  - [RelationCloseCleanup](../R/RelationCloseCleanup.md)
  - [MemoryContextReset](MemoryContextReset.md)
  - [AtAbort_Portals](../A/AtAbort_Portals.md)

## Notes and Other Information
- The function includes an assertion to validate that the input context is valid before proceeding
- The parent context specified in the parameter remains completely intact and usable after the operation
- Uses a while loop that relies on MemoryContextDelete's automatic delinking behavior
- This is a safer alternative to manual traversal and deletion when you only want to clear children
- Commonly used in portal management, relation cache cleanup, and transaction abort scenarios
- The operation is non-recursive in implementation but achieves recursive deletion through repeated calls to MemoryContextDelete

## Simplified Source

```c
// Simplified version of MemoryContextDeleteChildren
void MemoryContextDeleteChildren(MemoryContext context) {
    // Validate the input context
    Assert(MemoryContextIsValid(context));

    // Delete all children one by one
    // Note: Each deletion automatically unlinks the child from parent,
    // so we always delete the first child until none remain
    while (context->firstchild != NULL) {
        MemoryContextDelete(context->firstchild);
    }
}
```

Key simplifications made:
- Added clear explanatory comments for each major step
- Emphasized the automatic unlinking behavior that makes the algorithm work
- Maintained the exact same logic as the original (no actual simplification needed as the function is already optimal)
- Focused on clarity of the deletion strategy