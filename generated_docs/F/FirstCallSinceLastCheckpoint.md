# FirstCallSinceLastCheckpoint

## Location
[src/backend/postmaster/checkpointer.c:1336-1352](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/checkpointer.c#L1336-L1352)

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

## Simplified Source

```c
// Simplified version of FirstCallSinceLastCheckpoint
bool FirstCallSinceLastCheckpoint(void) {
    static int last_checkpoint_id = 0;
    int current_checkpoint_id;
    bool is_first_call = false;

    // Get current checkpoint completion counter from shared memory
    SpinLockAcquire(&CheckpointerShmem->ckpt_lck);
    current_checkpoint_id = CheckpointerShmem->ckpt_done;
    SpinLockRelease(&CheckpointerShmem->ckpt_lck);

    // Check if a new checkpoint has completed since last call
    if (current_checkpoint_id != last_checkpoint_id) {
        is_first_call = true;
    }

    // Update our local tracking variable
    last_checkpoint_id = current_checkpoint_id;

    return is_first_call;
}
```

Key simplifications made:
- Renamed variables for clarity (ckpt_done → last_checkpoint_id, new_done → current_checkpoint_id, FirstCall → is_first_call)
- Added explanatory comments for each logical step
- Maintained the essential algorithm: compare current checkpoint counter with locally stored value
- Preserved the spinlock mechanism for thread-safe shared memory access
- Focused on the core logic flow without losing any functionality