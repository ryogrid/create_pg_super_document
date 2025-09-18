# MemoryContextDeleteChildren

## Location
src/backend/utils/mmgr/mcxt.c: 539 - 567

## Overview
Deletes all descendants of the named context and releases all space allocated therein, while leaving the named context itself untouched.

## Definition


## Detailed Description
MemoryContextDeleteChildren provides a targeted deletion operation that removes all child contexts of a specified parent context while preserving the parent itself. This function is particularly useful when you need to clear all subordinate memory contexts but maintain the parent structure for continued use.

The implementation uses a simple but effective approach: it repeatedly deletes the first child until no children remain. This works because MemoryContextDelete automatically delinks each child from its parent during deletion, causing the next sibling to become the new first child. The loop continues until the firstchild pointer becomes NULL, indicating all children have been removed.

This function is commonly used in cleanup scenarios where the main context needs to persist but all its temporary or subsidiary contexts should be removed.

## Parameters / Member Variables
- : The parent memory context whose children will be deleted. The context itself remains valid and untouched.

## Dependencies
- Functions called/Symbols referenced:
  - MemoryContextIsValid
  - MemoryContextDelete
- Called from (representative examples):
  - PersistHoldablePortal
  - PortalRunMulti
  - RelationCloseCleanup
  - MemoryContextReset
  - AtAbort_Portals

## Notes and Other Information
- The function includes an assertion to validate that the input context is valid before proceeding
- The parent context specified in the parameter remains completely intact and usable after the operation
- Uses a while loop that relies on MemoryContextDelete's automatic delinking behavior
- This is a safer alternative to manual traversal and deletion when you only want to clear children
- Commonly used in portal management, relation cache cleanup, and transaction abort scenarios
- The operation is non-recursive in implementation but achieves recursive deletion through repeated calls to MemoryContextDelete