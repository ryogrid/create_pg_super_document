# heap_inplace_unlock

## Location
src/backend/access/heap/heapam.c: 6509 - 6522

## Overview
Releases locks acquired by heap_inplace_lock, serving as the cleanup counterpart to the inplace locking mechanism.

## Definition
```c
void heap_inplace_unlock(Relation relation, HeapTuple oldtup, Buffer buffer)
```

## Detailed Description
This function performs the essential cleanup operation that reverses the locking actions taken by heap_inplace_lock. It releases both the buffer-level exclusive lock and the tuple-level InplaceUpdateTupleLock in the proper order.

The function executes two key unlock operations:
1. Releases the exclusive buffer lock that was protecting the page containing the tuple
2. Releases the tuple-level lock that was preventing concurrent updates to the specific tuple

This function is designed to be called in three scenarios:
- After successful completion via heap_inplace_update_and_unlock
- When canceling an inplace update operation via systable_inplace_update_cancel
- As part of error handling when inplace operations need to be aborted

## Parameters / Member Variables
- `relation`: The heap relation containing the tuple
- `oldtup`: The HeapTuple that was locked for inplace update
- `buffer`: The buffer that was exclusively locked during the operation

## Dependencies
- Functions called/Symbols referenced:
  - LockBuffer (with BUFFER_LOCK_UNLOCK)
  - UnlockTuple (with InplaceUpdateTupleLock)
- Called from (representative examples):
  - heap_inplace_update_and_unlock
  - systable_inplace_update_cancel
  - HeapScanIsValid (indirect reference)

## Notes and Other Information
- Must be called to properly clean up after any successful heap_inplace_lock call
- Lock release order mirrors the reverse of the acquisition order in heap_inplace_lock
- Essential for preventing lock leaks in inplace update operations
- Simple but critical function for maintaining proper lock discipline
- Used both in success paths (after update completion) and error paths (operation cancellation)
- The function assumes the caller previously acquired the locks via heap_inplace_lock