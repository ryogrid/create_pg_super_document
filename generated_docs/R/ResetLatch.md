# ResetLatch

## Location
src/backend/storage/ipc/latch.c: 724 - 750

## Overview
ResetLatch clears a latch's signaled state, preparing it to block subsequent wait operations until the latch is set again.

## Definition


## Detailed Description
ResetLatch resets a latch to its non-signaled state, enabling subsequent calls to WaitLatch and related functions to properly block until the latch is set again. The function includes important safety checks to ensure that only the owning process can reset the latch and that the latch is not being reset while a wait operation might be in progress.

The function uses a memory barrier after clearing the latch state to ensure proper memory ordering. This barrier is crucial for preventing race conditions where a concurrent SetLatch operation might incorrectly conclude that it doesn't need to signal the latch, even though the resetting process might have missed seeing some flag updates that were supposed to trigger the latch.

## Parameters / Member Variables
- : Pointer to the Latch structure to reset to the non-signaled state

## Dependencies
- Functions called/Symbols referenced:
  - pg_memory_barrier (memory synchronization)
  - MyProcPid (current process ID for ownership verification)
  - Assert (debugging assertions)
- Called from (representative examples):
  - WaitForParallelWorkersToAttach (parallel processing coordination)
  - [BackgroundWriterMain](../B/BackgroundWriterMain.md) (background writer process main loop)
  - [CheckpointerMain](../C/CheckpointerMain.md) (checkpoint process main loop)
  - [WalReceiverMain](../W/WalReceiverMain.md) (WAL receiver main loop)
  - [LogicalRepApplyLoop](../L/LogicalRepApplyLoop.md) (logical replication worker loop)

## Notes and Other Information
- Only the latch owner (the process that owns the latch) should call this function
- The function includes assertions to verify ownership (owner_pid == MyProcPid) and that no wait operation is in progress (maybe_sleeping == false)
- A memory barrier is used after clearing the latch state to prevent race conditions with concurrent SetLatch operations
- This function is typically called in a loop pattern where the process resets the latch, checks conditions, and then waits if necessary
- The memory barrier ensures that any flag variable examinations after the reset will see the most recent values
- Used extensively in PostgreSQL's main loops for background processes and worker coordination