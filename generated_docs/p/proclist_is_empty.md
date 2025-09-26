# proclist_is_empty

## Location
[src/include/storage/proclist.h:38-47](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/proclist.h#L38-L47)

## Overview
Tests whether a proclist data structure is empty by checking if the head pointer indicates no valid processes.

## Definition
```c
static inline bool proclist_is_empty(const proclist_head *list)
```

## Detailed Description
The `proclist_is_empty` function is a static inline function that determines whether a process list (proclist) contains any processes. It performs this check by examining the head pointer of the list - if the head equals `INVALID_PROC_NUMBER`, the list is considered empty. This is an efficient O(1) operation that doesn't require traversing the entire list.

The function takes a const pointer parameter, indicating it does not modify the list structure and can be safely called on read-only list references. Being defined as static inline allows for optimal performance by eliminating function call overhead.

## Parameters / Member Variables
- `list`: A const pointer to the proclist_head structure to be checked for emptiness

## Dependencies
- Functions called/Symbols referenced:
  - `INVALID_PROC_NUMBER` (constant indicating an invalid process number)
  - `[proclist_head](proclist_head.md)` (data structure type)
  - `[proclist_node](proclist_node.md)` (referenced in the same file)
- Called from (representative examples):
  - `[ConditionVariableSignal](../C/ConditionVariableSignal.md)` (src/backend/storage/lmgr/condition_variable.c:265)
  - `[ConditionVariableBroadcast](../C/ConditionVariableBroadcast.md)` (src/backend/storage/lmgr/condition_variable.c:321, 324, 352)
  - `[LWLockWakeup](../L/LWLockWakeup.md)` (src/backend/storage/lmgr/lwlock.c:978, 997)
  - `[LWLockDequeueSelf](../L/LWLockDequeueSelf.md)` (src/backend/storage/lmgr/lwlock.c:1104)
  - [proclist_pop_head_node_offset](proclist_pop_head_node_offset.md) (src/include/storage/proclist.h:177)

## Notes and Other Information
- This is a static inline function defined in the header file for optimal performance
- The function is commonly used in synchronization and locking code to determine if any processes are waiting
- Returns true if the list is empty, false if it contains one or more processes
- The const parameter qualifier ensures the function is read-only and thread-safe for checking list state
- Often used before attempting to wake up processes or perform other list operations