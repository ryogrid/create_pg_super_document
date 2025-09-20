# proclist_init

## Location
[src/include/storage/proclist.h:29-37](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/proclist.h#L29-L37)

## Overview
Initializes a proclist data structure by setting both head and tail pointers to indicate an empty list.

## Definition

```c
static inline void
proclist_init(proclist_head *list)
```
## Detailed Description
The  function is a static inline function that initializes a process list (proclist) data structure. It sets both the head and tail pointers of the proclist to , which indicates that the list is empty. This function is typically called when creating or resetting a proclist to ensure it starts in a known, empty state.

The function is defined as static inline for performance reasons, allowing the compiler to optimize the simple initialization by expanding it inline at call sites rather than incurring function call overhead.

## Parameters / Member Variables
- : A pointer to the proclist_head structure to be initialized

## Dependencies
- Functions called/Symbols referenced:
  -  (constant indicating an invalid process number)
  -  (data structure type)
- Called from (representative examples):
  -  (src/backend/storage/lmgr/condition_variable.c:38)
  -  (src/backend/storage/lmgr/lwlock.c:716)
  -  (src/backend/storage/lmgr/lwlock.c:929)
  -  (src/backend/storage/lmgr/lwlock.c:1735)

## Notes and Other Information
- This is a static inline function defined in the header file for optimal performance
- The function is used in various locking and synchronization contexts throughout PostgreSQL
- After initialization, the proclist is ready to have processes added via other proclist operations
- The initialization ensures both head and tail pointers are in a consistent empty state