# _bt_upgradelockbufcleanup

## Location
src/backend/access/nbtree/nbtpage.c: 1109 - 1128

## Overview
_bt_upgradelockbufcleanup upgrades a standard B-tree buffer lock to a cleanup lock, providing exclusive access needed for maintenance operations like page cleaning and reorganization.

## Definition


## Detailed Description
_bt_upgradelockbufcleanup performs a lock upgrade operation on a B-tree buffer that is already pinned and locked with a standard read/write lock. The function first validates that the buffer memory is properly accessible using Valgrind instrumentation, then releases the current lock and immediately acquires a cleanup lock.

A cleanup lock is a special type of exclusive lock that provides stronger guarantees than regular write locks. While a regular write lock allows concurrent readers, a cleanup lock blocks all other access (both readers and writers) to the buffer. This level of exclusivity is necessary for operations that need to reorganize page contents, remove dead tuples, or perform other maintenance tasks that could leave the page temporarily in an inconsistent state.

The upgrade is atomic in the sense that the function maintains continuous exclusive access throughout the operation, preventing other backends from accessing the buffer during the transition.

## Parameters / Member Variables
- : The relation (table/index) that owns the buffer
- : The buffer descriptor for the page whose lock should be upgraded

## Dependencies
- Functions called/Symbols referenced:
  - VALGRIND_CHECK_MEM_IS_DEFINED
  - [BufferGetPage](../B/BufferGetPage.md)
  - [LockBuffer](../L/LockBuffer.md)
  - BUFFER_LOCK_UNLOCK
  - LockBufferForCleanup

- Called from (representative examples):
  - [btvacuumpage](btvacuumpage.md)

## Notes and Other Information
- Buffer must already be pinned and locked by the calling backend before calling this function
- Provides stronger exclusivity than regular write locks - blocks all concurrent access including readers
- Used primarily during VACUUM operations and other B-tree maintenance tasks
- The lock upgrade is atomic - no window exists where the buffer is unlocked
- Cleanup locks are necessary for operations that temporarily leave pages in inconsistent states
- Valgrind memory validation ensures buffer accessibility before proceeding with lock operations
- After acquiring cleanup lock, the calling backend has exclusive access until explicitly releasing or downgrading the lock