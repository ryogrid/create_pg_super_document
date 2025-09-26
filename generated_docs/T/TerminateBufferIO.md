# TerminateBufferIO

## Location
src/backend/storage/buffer/bufmgr.c: 5589 - 5625

## Overview
TerminateBufferIO completes I/O operations on a buffer by clearing the BM_IO_IN_PROGRESS flag, updating buffer state flags, and notifying waiting processes.

## Definition
```c
static void TerminateBufferIO(BufferDesc *buf, bool clear_dirty, uint32 set_flag_bits, bool forget_owner)
```

## Detailed Description
This function serves as the completion handler for buffer I/O operations, responsible for cleaning up state and coordinating with other processes waiting for I/O completion. It performs several critical tasks:

1. **State cleanup**: Clears the BM_IO_IN_PROGRESS flag and optionally BM_IO_ERROR
2. **Dirty flag management**: Conditionally clears BM_DIRTY and BM_CHECKPOINT_NEEDED flags based on successful writes
3. **Flag updates**: Applies additional status flags (like BM_VALID for successful reads or BM_IO_ERROR for failures)
4. **Resource management**: Optionally releases buffer I/O tracking from the resource owner
5. **Process coordination**: Broadcasts to condition variable to wake up waiting processes

The clear_dirty parameter enables proper handling of write operations by clearing dirty flags only when appropriate. The BM_JUST_DIRTIED check prevents race conditions where the buffer was re-dirtied during the write operation.

## Parameters / Member Variables
- `buf`: Pointer to the BufferDesc for which I/O is being terminated
- `clear_dirty`: If true, clear BM_DIRTY flag for successful writes (when BM_JUST_DIRTIED is not set)
- `set_flag_bits`: Additional flags to set (e.g., BM_VALID for successful reads, BM_IO_ERROR for failures)
- `forget_owner`: If true, release buffer I/O from current resource owner tracking

## Dependencies
- Functions called/Symbols referenced:
  - BufferDesc
  - LockBufHdr
  - UnlockBufHdr
  - BM_IO_IN_PROGRESS
  - BM_IO_ERROR
  - BM_JUST_DIRTIED
  - BM_DIRTY
  - BM_CHECKPOINT_NEEDED
  - ResourceOwnerForgetBufferIO
  - BufferDescriptorGetBuffer
  - ConditionVariableBroadcast
  - BufferDescriptorGetIOCV
- Called from (representative examples):
  - BufferIsPinned
  - ZeroAndLockBuffer
  - WaitReadBuffers
  - ExtendBufferedRelShared
  - FlushBuffer
  - AbortBufferIO

## Notes and Other Information
- Assumes the calling process is currently executing I/O for the buffer and BM_IO_IN_PROGRESS is set
- Always clears BM_IO_ERROR flag to reset error state from previous operations
- The condition variable broadcast is crucial for waking up processes blocked in WaitIO
- Resource owner forgetting is optional and controlled by the forget_owner parameter
- Used for both successful I/O completion and error handling scenarios
- Part of PostgreSQL's buffer I/O completion infrastructure
- The buffer must be pinned when this function is called