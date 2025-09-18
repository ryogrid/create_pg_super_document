# FirstCallSinceLastCheckpoint

## Location
src/backend/postmaster/checkpointer.c: 1336 - 1352

## Overview
FirstCallSinceLastCheckpoint allows a process to detect when a new checkpoint cycle has begun and take action once per checkpoint cycle through asynchronous checkpoint completion checking.

## Definition
bool FirstCallSinceLastCheckpoint(void)

## Detailed Description
This function provides a mechanism for background processes to synchronize their operations with checkpoint cycles. It enables processes to perform certain actions exactly once per checkpoint by detecting when a new checkpoint has completed since the last time the function was called.

The function works by maintaining a static local variable that tracks the checkpoint completion counter from shared memory. Each time the function is called, it compares the current checkpoint completion counter from shared memory with its locally stored value. If the values differ, it indicates that a new checkpoint has completed since the last call, and the function returns true for this first call in the new checkpoint cycle.

The implementation uses spinlocks for efficient, short-duration access to the shared memory checkpoint completion counter, ensuring atomic reads of the checkpoint state without the overhead of heavier synchronization mechanisms.

## Parameters / Member Variables
This function takes no parameters and maintains internal state through a static variable:
- ckpt_done: Static variable storing the last known checkpoint completion counter

## Dependencies
- Functions called/Symbols referenced:
  - SpinLockAcquire
  - SpinLockRelease  
- Shared memory accessed:
  - CheckpointerShmem->ckpt_lck (spinlock)
  - CheckpointerShmem->ckpt_done (completion counter)
- Called from:
  - [BackgroundWriterMain](../B/BackgroundWriterMain.md)

## Notes and Other Information
- Returns true only on the first call after a checkpoint completes, false on subsequent calls until the next checkpoint
- Uses static storage to maintain state across function calls, making it inherently per-process
- The spinlock ensures atomic access to the checkpoint completion counter
- Designed for lightweight, frequent polling by background processes
- The function is thread-safe due to the spinlock protection around shared memory access
- Enables checkpoint-synchronized operations without requiring explicit checkpoint completion notifications