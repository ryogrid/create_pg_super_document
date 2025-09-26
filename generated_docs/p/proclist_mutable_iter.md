# proclist_mutable_iter

## Location
src/include/storage/proclist_types.h: 47 - 51

## Overview
A structure representing an iterator for traversing doubly-linked process lists that allows safe modifications (such as deletions) during iteration.

## Definition


## Detailed Description
The `proclist_mutable_iter` structure provides a safe way to iterate through a doubly-linked list of PostgreSQL processes while allowing modifications to the list during traversal. This is particularly important when you need to remove processes from the list as you encounter them, which would normally invalidate standard iterators.

Key features:
1. **Safe Modification**: Pre-fetches the next element before processing the current one, allowing safe deletion of the current element
2. **Process Index-Based**: Uses `ProcNumber` indexes consistent with the proclist system
3. **Forward Iteration**: Supports forward traversal through the list
4. **Modification-Tolerant**: Handles list modifications (especially deletions) gracefully during iteration

The iterator works by maintaining both the current position and pre-fetching the next position, so when the current element is removed from the list, the iteration can safely continue.

## Parameters / Member Variables
- `cur`: ProcNumber (0-based PGPROC index) of the current process being processed in the iteration
- `next`: ProcNumber (0-based PGPROC index) of the next process to be processed, pre-fetched to allow safe modification of the current element

## Dependencies
- Functions called/Symbols referenced:
  - ProcNumber (typedef used for cur/next fields)
- Called from (representative examples):
  - LWLockWakeup
  - LWLockUpdateVar
  - proclist_foreach_modify

## Notes and Other Information
- Essential for scenarios where processes need to be removed from wait lists during traversal
- The pre-fetching strategy ensures that removing the current element doesn't break the iteration
- Used extensively in PostgreSQL's lock management system when waking up waiting processes
- Works in conjunction with `proclist_head` and `proclist_node` to provide complete list management functionality
- The iterator pattern allows for complex operations like selectively waking processes based on certain criteria while maintaining list integrity