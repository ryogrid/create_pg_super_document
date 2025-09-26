# LockBufferForCleanup

## Location
[src/backend/storage/buffer/bufmgr.c:5212-5346](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L5212-L5346)

## Overview
LockBufferForCleanup acquires an exclusive lock on a buffer and waits until no other backends hold pins on it, enabling safe deletion of items from the buffer.

## Definition

```c
void
LockBufferForCleanup(Buffer buffer)
```
## Detailed Description
This function implements a specialized locking protocol for buffer cleanup operations. It ensures that when a backend wants to delete items from a disk page, it can do so safely by: (1) acquiring an exclusive lock on the buffer, and (2) waiting until the pin count drops to 1 (meaning only the current backend holds a pin).

The function loops until it successfully observes a pin count of 1 while holding the exclusive lock. If other backends have pins on the buffer, it marks itself as waiting for the pin count to drop and blocks until signaled by UnpinBuffer() calls from other backends. This prevents race conditions where other backends might have pointers into the buffer during cleanup operations.

For hot standby scenarios, the function includes additional logic to handle recovery conflicts and logging, publishing buffer wait information for the startup process and resolving conflicts appropriately.

## Parameters / Member Variables
- `buffer`: The buffer to lock for cleanup operations (must already be pinned by the caller)

## Dependencies
- Functions called/Symbols referenced:
  - BufferIsPinned
  - [CheckBufferIsPinnedOnce](../C/CheckBufferIsPinnedOnce.md)
  - BufferIsLocal
  - [GetBufferDescriptor](../G/GetBufferDescriptor.md)
  - [LockBuffer](LockBuffer.md)
  - [LockBufHdr](LockBufHdr.md)/UnlockBufHdr
  - BUF_STATE_GET_REFCOUNT
  - [LogRecoveryConflict](LogRecoveryConflict.md)
  - [SetStartupBufferPinWaitBufId](../S/SetStartupBufferPinWaitBufId.md)
  - [ResolveRecoveryConflictWithBufferPin](../R/ResolveRecoveryConflictWithBufferPin.md)
  - [ProcWaitForSignal](../P/ProcWaitForSignal.md)
- Called from (representative examples):
  - [ginVacuumPostingTree](../g/ginVacuumPostingTree.md)
  - [hashbulkdelete](../h/hashbulkdelete.md)
  - [lazy_scan_heap](../l/lazy_scan_heap.md)
  - [_bt_upgradelockbufcleanup](../b/_bt_upgradelockbufcleanup.md)
  - [ZeroAndLockBuffer](../Z/ZeroAndLockBuffer.md)

## Notes and Other Information
- Requires the buffer to be pinned exactly once by the calling backend before invocation
- Returns immediately for local buffers since they don't have cross-backend pin conflicts
- Uses BM_PIN_COUNT_WAITER flag to coordinate with other potential waiters
- Includes comprehensive recovery conflict handling for hot standby scenarios
- The protocol prevents the ABA problem where items might be reused between observation and deletion
- Process title is updated to show "waiting" status during pin count waits