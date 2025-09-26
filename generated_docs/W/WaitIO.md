# WaitIO

## Location
[src/backend/storage/buffer/bufmgr.c:5483-5531](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L5483-L5531)

## Overview
WaitIO blocks the current process until the IO_IN_PROGRESS flag on a specified buffer is cleared, ensuring that I/O operations complete before proceeding.

## Definition
```c
static void WaitIO(BufferDesc *buf)
```

## Detailed Description
This function implements a blocking wait mechanism for buffer I/O completion. It continuously polls the buffer's state until the BM_IO_IN_PROGRESS flag is cleared, indicating that any ongoing I/O operation on the buffer has finished.

The function uses PostgreSQL's condition variable mechanism for efficient waiting:
1. Prepares to sleep on the buffer's I/O condition variable
2. Enters a loop that checks the buffer state
3. If I/O is still in progress, sleeps on the condition variable
4. Wakes up when signaled and rechecks the buffer state
5. Exits when I/O is complete and cleans up the condition variable

The function uses spinlock protection when checking the buffer state to ensure correctness, even though this might not always be strictly necessary.

## Parameters / Member Variables
- `buf`: Pointer to the BufferDesc for which to wait for I/O completion

## Dependencies
- Functions called/Symbols referenced:
  - [BufferDesc](../B/BufferDesc.md)
  - [BufferDescriptorGetIOCV](../B/BufferDescriptorGetIOCV.md)
  - ConditionVariable
  - [ConditionVariablePrepareToSleep](../C/ConditionVariablePrepareToSleep.md)
  - [LockBufHdr](../L/LockBufHdr.md)
  - [UnlockBufHdr](../U/UnlockBufHdr.md)
  - BM_IO_IN_PROGRESS
  - [ConditionVariableSleep](../C/ConditionVariableSleep.md)
  - [ConditionVariableCancelSleep](../C/ConditionVariableCancelSleep.md)
- Called from (representative examples):
  - BufferIsPinned
  - [InvalidateBuffer](../I/InvalidateBuffer.md)
  - [StartBufferIO](../S/StartBufferIO.md)

## Notes and Other Information
- This function is static and only used internally within the buffer manager
- Only applies to shared buffers, not local buffers
- Assumes that nested buffer I/O never occurs (at most one BM_IO_IN_PROGRESS bit per process)
- Uses WAIT_EVENT_BUFFER_IO as the wait event type for monitoring and debugging
- The spinlock acquisition during state checking ensures memory ordering and correctness
- Part of PostgreSQL's buffer I/O handling infrastructure