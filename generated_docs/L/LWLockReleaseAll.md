# LWLockReleaseAll

## Location
[src/backend/storage/lmgr/lwlock.c:1878-1894](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lwlock.c#L1878-L1894)

## Overview
Releases all currently-held LWLocks by the current process, primarily used for cleanup during error recovery without affecting the interrupt holdoff count.

## Definition
```c
void LWLockReleaseAll(void)
```

## Detailed Description
LWLockReleaseAll is a cleanup function that releases all LWLocks currently held by the calling process. It is primarily designed for error recovery scenarios where the process needs to clean up its lock state after an ereport(ERROR) or similar exception. 

A critical aspect of this function is that it preserves the InterruptHoldoffCount, unlike individual LWLockRelease calls. This is essential during error recovery because the InterruptHoldoffCount has already been set to an appropriate level earlier in the error recovery process. If the function decremented the interrupt holdoff count for each released lock, it could drive the count below zero, disrupting the error handling mechanism.

The function operates by iterating through the held_lwlocks array from the most recently acquired lock backward, calling HOLD_INTERRUPTS() before each LWLockRelease() call to maintain proper interrupt handling.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - HOLD_INTERRUPTS: Prevents interrupt processing during lock release
  - LWLockRelease: Releases individual locks from the held_lwlocks array
- Global variables used:
  - num_held_lwlocks: Count of currently held locks
  - held_lwlocks: Array tracking held locks
- Called from (representative examples):
  - AbortTransaction: Transaction abort cleanup
  - AbortSubTransaction: Subtransaction abort cleanup
  - ShutdownAuxiliaryProcess: Process shutdown cleanup
  - BackgroundWriterMain: Background writer error handling
  - CheckpointerMain: Checkpointer error handling
  - WalWriterMain: WAL writer error handling
  - ProcKill: Process termination cleanup

## Notes and Other Information
- This function is specifically designed for error recovery scenarios and maintains proper interrupt handling semantics
- Unlike normal LWLockRelease calls, this function does not decrement InterruptHoldoffCount
- The function processes held locks in reverse order (LIFO - Last In, First Out)
- Used extensively in PostgreSQL's background processes for error cleanup
- Critical for maintaining system stability during abnormal process termination
- Located in src/backend/storage/lmgr/lwlock.c:1878-1894