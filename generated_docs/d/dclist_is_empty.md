# dclist_is_empty

## Location
src/include/lib/ilist.h: 682 - 692

## Overview
Returns true if the doubly-linked count list is empty, otherwise false, providing an O(1) emptiness check by examining the element count.

## Definition
```c
static inline bool
dclist_is_empty(const dclist_head *head)
```

## Detailed Description
This function checks whether a doubly-linked count list (dclist) contains any elements by examining the count field. It provides an O(1) constant-time check for list emptiness, which is more efficient than traversing the underlying doubly-linked list. The function includes an assertion that verifies the consistency between the count field and the actual state of the underlying dlist structure, ensuring data structure integrity during debugging builds.

## Parameters / Member Variables
- `head`: Pointer to the dclist_head structure to check for emptiness (const qualified as the function doesn't modify the list)

## Dependencies
- Functions called/Symbols referenced:
  - dlist_is_empty (used in assertion for consistency checking)
  - dclist_head (parameter type)
- Called from (representative examples):
  - RemoveFromWaitQueue (src/backend/storage/lmgr/lock.c:1919)
  - ProcSleep (src/backend/storage/lmgr/proc.c:1130)
  - ProcLockWakeup (src/backend/storage/lmgr/proc.c:1717)
  - dclist_head_element_off (src/include/lib/ilist.h:890)
  - dclist_tail_element_off (src/include/lib/ilist.h:911)

## Notes and Other Information
- Provides O(1) performance compared to O(n) traversal of standard dlist
- Includes assertion to verify consistency between count and underlying dlist state
- The function is static inline for performance optimization
- Parameter is const-qualified indicating the function doesn't modify the list
- Part of PostgreSQL's doubly-linked count list implementation in src/include/lib/ilist.h
- Commonly used in lock management and process synchronization code